import 'dart:convert';
import 'dart:io';

import 'package:ndjson_ingest/ndjson_ingest.dart';
import 'package:sqlite3/sqlite3.dart';

/// Measures imports including source hashing; all generated files are temporary.
Future<void> main() async {
  const rows = 100000;
  final temp = await Directory.systemTemp.createTemp('ndjson-benchmark-');
  try {
    final source = File('${temp.path}/source.ndjson');
    final sink = source.openWrite();
    try {
      for (var id = 0; id < rows; id++) {
        sink.writeln(jsonEncode({'id': id, 'value': 'Cà phê $id'}));
        if (id % 1000 == 0) await sink.flush();
      }
    } finally {
      await sink.close();
    }
    final sourceBytes = await source.length();
    for (final batchRows in [100, 500, 2000]) {
      final target = '${temp.path}/batch-$batchRows.sqlite';
      final db = sqlite3.open(target);
      try {
        db.execute('CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)');
      } finally {
        db.close();
      }
      final timer = Stopwatch()..start();
      final result = await importNdjson(
        sourcePath: source.path,
        databasePath: target,
        jobId: 'benchmark',
        table: 'records',
        columns: const {'id': 'id', 'value': 'value'},
        batchRows: batchRows,
      );
      timer.stop();
      if (!result.complete || result.rows != rows) {
        throw StateError('Benchmark import did not complete.');
      }
      print(
        jsonEncode({
          'batchRows': batchRows,
          'rows': result.rows,
          'sourceBytes': sourceBytes,
          'elapsedMs': timer.elapsedMilliseconds,
          'rowsPerSecond': (rows * 1000000 / timer.elapsedMicroseconds).round(),
          // Process lifetime high-water mark, not incremental batch memory.
          'processPeakRssBytes': ProcessInfo.maxRss,
        }),
      );
    }
  } finally {
    await temp.delete(recursive: true);
  }
}
