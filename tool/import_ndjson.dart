import 'dart:convert';
import 'dart:io';

import 'package:ndjson_ingest/ndjson_ingest.dart';
import 'package:sqlite3/sqlite3.dart';

Future<void> main(List<String> args) async {
  final limit = args.length == 4 ? int.tryParse(args[3]) : null;
  if ((args.length != 3 && args.length != 4) ||
      (args.length >= 3 && args[2].trim().isEmpty) ||
      (args.length == 4 && (limit == null || limit < 1))) {
    stderr.writeln(
      'Usage: dart run tool/import_ndjson.dart '
      '<source.ndjson> <target.sqlite> <job-id> [max-batches]',
    );
    exitCode = 64;
    return;
  }

  final [sourcePath, databasePath, jobId, ..._] = args;
  try {
    if (!await File(sourcePath).exists()) {
      throw StateError('Could not read the import source.');
    }
    if (await File(databasePath).exists() &&
        await FileSystemEntity.identical(sourcePath, databasePath)) {
      throw StateError('The source and database must be different files.');
    }
    final database = sqlite3.open(databasePath);
    try {
      database.execute('''
        CREATE TABLE IF NOT EXISTS products (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL
        )
      ''');
    } finally {
      database.close();
    }
    final progress = await importNdjson(
      sourcePath: sourcePath,
      databasePath: databasePath,
      jobId: jobId,
      table: 'products',
      columns: const {'id': 'id', 'name': 'name'},
      maxBatches: limit,
    );
    stdout.writeln(
      jsonEncode({
        'rows': progress.rows,
        'nextOffset': progress.offset,
        'complete': progress.complete,
      }),
    );
  } on SqliteException catch (error) {
    stderr.writeln('SQLite import failed (code ${error.extendedResultCode}).');
    exitCode = 1;
  } on FormatException catch (error) {
    stderr.writeln(error.message);
    exitCode = 1;
  } on StateError catch (error) {
    stderr.writeln(error.message);
    exitCode = 1;
  } on FileSystemException {
    stderr.writeln('Could not read the import source.');
    exitCode = 1;
  }
}
