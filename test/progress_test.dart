import 'dart:convert';
import 'dart:io';
import 'dart:isolate';

import 'package:ndjson_ingest/ndjson_ingest.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

void main() {
  late Directory directory;
  late File source;
  late String target;
  setUp(() {
    directory = Directory.systemTemp.createTempSync('ndjson-progress-');
    source = File('${directory.path}/source.ndjson');
    target = '${directory.path}/target.sqlite';
    final db = sqlite3.open(target);
    try {
      db.execute(
        'CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)',
      );
    } finally {
      db.close();
    }
  });
  tearDown(() => directory.deleteSync(recursive: true));

  void writeRows(int count) => source.writeAsStringSync(
    [
      for (var id = 0; id < count; id++)
        '${jsonEncode({'id': id, 'value': 'Cà phê $id'})}${id.isEven ? '\n' : '\r\n'}',
    ].join(),
  );

  Future<ImportProgress> run({
    int batchRows = 2,
    int? maxBatches,
    String mappingVersion = '1',
    void Function(ImportProgress)? onProgress,
  }) => importNdjson(
    sourcePath: source.path,
    databasePath: target,
    jobId: 'progress',
    table: 'records',
    columns: const {'id': 'id', 'value': 'value'},
    mappingVersion: mappingVersion,
    batchRows: batchRows,
    maxBatches: maxBatches,
    onProgress: onProgress,
  );

  void checkCheckpoint(ImportProgress snapshot, {bool exact = true}) {
    final db = sqlite3.open(target);
    try {
      db.execute('PRAGMA busy_timeout = 5000');
      final saved = db
          .select('SELECT rows_committed, next_offset FROM ndjson_import_jobs')
          .single;
      expect(
        saved['rows_committed'],
        exact ? snapshot.rows : greaterThanOrEqualTo(snapshot.rows),
      );
      expect(
        saved['next_offset'],
        exact ? snapshot.offset : greaterThanOrEqualTo(snapshot.offset),
      );
      expect(
        db.select('SELECT count(*) AS n FROM records').single['n'],
        exact ? snapshot.rows : greaterThanOrEqualTo(snapshot.rows),
      );
    } finally {
      db.close();
    }
  }

  test(
    'callbacks stay in the caller and report only committed boundaries',
    () async {
      writeRows(7);
      final snapshots = <ImportProgress>[];
      final caller = Isolate.current.controlPort;
      final nonSendable = ReceivePort();
      try {
        final result = await run(
          onProgress: (snapshot) {
            expect(Isolate.current.controlPort, caller);
            expect(nonSendable.sendPort, isA<SendPort>());
            checkCheckpoint(snapshot, exact: false);
            snapshots.add(snapshot);
          },
        );
        expect(snapshots.first, (rows: 0, offset: 0, complete: false));
        expect(snapshots.last, result);
        expect(result, (rows: 7, offset: source.lengthSync(), complete: true));
        final bytes = source.readAsBytesSync();
        final offsets = [
          0,
          for (var i = 0; i < bytes.length; i++)
            if (bytes[i] == 10) i + 1,
        ];
        for (var i = 0; i < snapshots.length; i++) {
          final current = snapshots[i];
          expect(current.offset, offsets[current.rows]);
          expect(current.rows == 7 || current.rows.isEven, isTrue);
          if (i > 0) {
            expect(current.rows, greaterThan(snapshots[i - 1].rows));
            expect(current.offset, greaterThan(snapshots[i - 1].offset));
          }
        }
        final count = snapshots.length;
        await Future<void>.delayed(Duration.zero);
        expect(snapshots, hasLength(count));
        checkCheckpoint(result);
      } finally {
        nonSendable.close();
      }
    },
  );

  test('pause, resume and completed jobs deliver their final result', () async {
    writeRows(7);
    final snapshots = <ImportProgress>[];
    final paused = await run(maxBatches: 1, onProgress: snapshots.add);
    expect(paused.rows, 2);
    expect(paused.complete, isFalse);
    expect(snapshots.last, paused);
    checkCheckpoint(paused);
    snapshots.clear();
    final resumed = await run(batchRows: 3, onProgress: snapshots.add);
    expect(snapshots.first, paused);
    expect(snapshots.last, resumed);
    expect(resumed.complete, isTrue);
    snapshots.clear();
    expect(await run(onProgress: snapshots.add), resumed);
    expect(snapshots, [resumed]);
  });

  test('empty files report one completed snapshot', () async {
    source.writeAsStringSync('');
    final snapshots = <ImportProgress>[];
    final result = await run(onProgress: snapshots.add);
    expect(result, (rows: 0, offset: 0, complete: true));
    expect(snapshots, [result]);
    checkCheckpoint(result);
  });

  test('validation failure flushes the last committed progress', () async {
    writeRows(3);
    final committedOffset =
        source.readAsBytesSync().indexOf(
          10,
          source.readAsBytesSync().indexOf(10) + 1,
        ) +
        1;
    source.writeAsStringSync('{"id":4,"value":[]}\n', mode: FileMode.append);
    final snapshots = <ImportProgress>[];
    await expectLater(
      run(onProgress: snapshots.add),
      throwsA(
        isA<NdjsonImportException>().having(
          (e) => e.code,
          'code',
          NdjsonImportErrorCode.unsupportedValue,
        ),
      ),
    );
    expect(snapshots.last, (rows: 2, offset: committedOffset, complete: false));
    checkCheckpoint(snapshots.last);
    final count = snapshots.length;
    await Future<void>.delayed(Duration.zero);
    expect(snapshots, hasLength(count));
  });

  test('SQLite rollback never reports the rejected batch', () async {
    writeRows(4);
    final committedOffset = source.lengthSync();
    source.writeAsStringSync(
      '{"id":0,"value":"duplicate"}\n',
      mode: FileMode.append,
    );
    final snapshots = <ImportProgress>[];
    await expectLater(
      run(onProgress: snapshots.add),
      throwsA(
        isA<NdjsonImportException>().having((e) => e.sqliteCode, 'code', 1555),
      ),
    );
    expect(snapshots.last, (rows: 4, offset: committedOffset, complete: false));
    checkCheckpoint(snapshots.last);
  });

  test('I/O failure before verification emits no progress', () async {
    final snapshots = <ImportProgress>[];
    await expectLater(
      run(onProgress: snapshots.add),
      throwsA(
        isA<NdjsonImportException>().having(
          (e) => e.code,
          'code',
          NdjsonImportErrorCode.io,
        ),
      ),
    );
    expect(snapshots, isEmpty);
  });

  test('incompatible resume emits no misleading initial progress', () async {
    writeRows(5);
    await run(maxBatches: 1);
    final snapshots = <ImportProgress>[];
    await expectLater(
      run(mappingVersion: '2', onProgress: snapshots.add),
      throwsStateError,
    );
    expect(snapshots, isEmpty);
  });

  test(
    'callback failure waits for cleanup and does not cancel the import',
    () async {
      writeRows(5);
      final failure = StateError('Application observer failed');
      var calls = 0;
      await expectLater(
        run(
          onProgress: (_) {
            calls++;
            throw failure;
          },
        ),
        throwsA(same(failure)),
      );
      expect(calls, 1);
      final completed = await run();
      expect(completed.rows, 5);
      expect(completed.complete, isTrue);
      checkCheckpoint(completed);
    },
  );

  test('worker failure takes precedence over a callback failure', () async {
    writeRows(3);
    source.writeAsStringSync('[]\n', mode: FileMode.append);
    var calls = 0;
    await expectLater(
      run(
        onProgress: (_) {
          calls++;
          throw StateError('Application observer failed');
        },
      ),
      throwsA(
        isA<NdjsonImportException>().having(
          (e) => e.code,
          'code',
          NdjsonImportErrorCode.expectedObject,
        ),
      ),
    );
    expect(calls, 1);
    final db = sqlite3.open(target);
    try {
      expect(db.select('SELECT count(*) AS n FROM records').single['n'], 2);
    } finally {
      db.close();
    }
  });

  test(
    'many small batches throttle events while always delivering the end',
    () async {
      writeRows(1000);
      final snapshots = <ImportProgress>[];
      final watch = Stopwatch()..start();
      final result = await run(batchRows: 1, onProgress: snapshots.add);
      watch.stop();
      expect(
        snapshots.length,
        lessThanOrEqualTo(watch.elapsedMilliseconds ~/ 100 + 2),
      );
      expect(snapshots.first.rows, 0);
      expect(snapshots.last, result);
      expect(result.rows, 1000);
    },
  );
}
