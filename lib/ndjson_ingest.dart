import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:sqlite3/sqlite3.dart';

const _maxLineBytes = 256 * 1024;
const _maxBatchBytes = 4 * 1024 * 1024;
const _batchRows = 500;

/// The cumulative progress stored for an import job.
typedef ImportProgress = ({int rows, int offset, bool complete});

typedef _Line = ({List<Object?> values, int nextOffset});

/// Imports an immutable local NDJSON file into an existing SQLite table.
///
/// [columns] maps top-level JSON keys to SQLite column names. Mapped values
/// must be strings, numbers, booleans, or null. Booleans are bound as 0 or 1.
/// Every mapped key must exist in each row. SQLite enforces table constraints.
///
/// Use the same [jobId] and configuration to resume. Change [mappingVersion]
/// when the target schema changes the meaning of imported data. [maxBatches]
/// can pause the import at a committed boundary. The result is cumulative.
Future<ImportProgress> importNdjson({
  required String sourcePath,
  required String databasePath,
  required String jobId,
  required String table,
  required Map<String, String> columns,
  String mappingVersion = '1',
  int? maxBatches,
}) {
  if (jobId.trim().isEmpty) throw ArgumentError('jobId must not be blank.');
  if (maxBatches != null && maxBatches < 1) {
    throw ArgumentError('maxBatches must be positive.');
  }
  if (columns.isEmpty || mappingVersion.trim().isEmpty) {
    throw ArgumentError('Provide columns and a nonblank mappingVersion.');
  }
  final mapping = Map<String, String>.of(columns);
  _identifier(table);
  final destinations = <String>{};
  for (final column in mapping.values) {
    _identifier(column);
    if (!destinations.add(column.toLowerCase())) {
      throw ArgumentError('Destination columns must be distinct.');
    }
  }
  final tableName = table.toLowerCase();
  if (tableName == 'ndjson_import_jobs' || tableName.startsWith('sqlite_')) {
    throw ArgumentError('The destination table name is reserved.');
  }
  return Isolate.run(
    () => _import(
      sourcePath,
      databasePath,
      jobId,
      table,
      mapping,
      mappingVersion,
      maxBatches,
    ),
  );
}

String _identifier(String name) {
  if (name.trim().isEmpty || name.contains(String.fromCharCode(0))) {
    throw ArgumentError(
      'SQLite identifiers must be nonblank and contain no NUL.',
    );
  }
  return '"${name.replaceAll('"', '""')}"';
}

String _mappingDigest(
  String table,
  Map<String, String> columns,
  String version,
) {
  final keys = columns.keys.toList()..sort();
  final config = [
    table,
    version,
    {for (final key in keys) key: columns[key]},
  ];
  return sha256.convert(utf8.encode(jsonEncode(config))).toString();
}

Future<ImportProgress> _import(
  String sourcePath,
  String databasePath,
  String jobId,
  String table,
  Map<String, String> columns,
  String mappingVersion,
  int? maxBatches,
) async {
  final file = File(sourcePath);
  final size = await file.length();
  if (await File(databasePath).exists() &&
      await FileSystemEntity.identical(sourcePath, databasePath)) {
    throw StateError('The source and database must be different files.');
  }
  final digest = (await sha256.bind(file.openRead()).single).toString();
  final mappingDigest = _mappingDigest(table, columns, mappingVersion);
  final database = sqlite3.open(databasePath, mode: OpenMode.readWrite);
  try {
    database.execute('PRAGMA busy_timeout = 5000');
    database.execute('PRAGMA foreign_keys = ON');
    final fields = columns.values.map(_identifier).join(', ');
    final parameters = List.filled(columns.length, '?').join(', ');
    final insert = database.prepare(
      'INSERT INTO ${_identifier(table)} ($fields) VALUES ($parameters)',
    );
    try {
      final saved = _openJob(database, jobId, digest, mappingDigest);
      if (saved['source_sha256'] != digest) {
        throw StateError('The source changed. Start a new import job.');
      }
      if (saved['mapping_sha256'] != mappingDigest) {
        throw StateError('The import mapping changed. Start a new import job.');
      }
      var offset = saved['next_offset'] as int;
      var rows = saved['rows_committed'] as int;
      if (offset < 0 || offset > size || rows < 0) {
        throw StateError('Invalid import checkpoint.');
      }
      if (offset == size) return (rows: rows, offset: offset, complete: true);

      var batches = 0;
      var pendingOffset = offset;
      final batch = <List<Object?>>[];

      void commit() {
        database.execute('BEGIN IMMEDIATE');
        final nextRows = rows + batch.length;
        try {
          final current = _checkpoint(database, jobId);
          if (current['next_offset'] != offset ||
              current['rows_committed'] != rows ||
              current['source_sha256'] != digest ||
              current['mapping_sha256'] != mappingDigest) {
            throw StateError('Another worker changed the import checkpoint.');
          }
          for (final values in batch) {
            insert.execute(values);
          }
          database.execute(
            '''
            UPDATE ndjson_import_jobs
            SET next_offset = ?, rows_committed = ?
            WHERE job_id = ?
          ''',
            [pendingOffset, nextRows, jobId],
          );
          database.execute('COMMIT');
        } catch (_) {
          if (!database.autocommit) database.execute('ROLLBACK');
          rethrow;
        }
        rows = nextRows;
        offset = pendingOffset;
        batches++;
        batch.clear();
      }

      await for (final line in _readLines(
        file,
        offset,
        rows + 1,
        columns.keys,
      )) {
        if (batch.isNotEmpty && line.nextOffset - offset > _maxBatchBytes) {
          commit();
          if (batches == maxBatches) break;
        }
        batch.add(line.values);
        pendingOffset = line.nextOffset;
        if (batch.length == _batchRows) {
          commit();
          if (batches == maxBatches) break;
        }
      }
      if (batch.isNotEmpty) commit();
      return (rows: rows, offset: offset, complete: offset == size);
    } finally {
      insert.close();
    }
  } finally {
    database.close();
  }
}

Row _openJob(Database database, String jobId, String digest, String mapping) {
  database.execute('BEGIN IMMEDIATE');
  try {
    database.execute('''
      CREATE TABLE IF NOT EXISTS ndjson_import_jobs (
        job_id TEXT PRIMARY KEY NOT NULL,
        source_sha256 TEXT NOT NULL,
        mapping_sha256 TEXT NOT NULL,
        next_offset INTEGER NOT NULL,
        rows_committed INTEGER NOT NULL
      )
    ''');
    final columns = database.select('PRAGMA table_info(ndjson_import_jobs)');
    if (!columns.any((column) => column['name'] == 'mapping_sha256')) {
      // The original CLI only imported this fixed mapping.
      final legacy = _mappingDigest('products', {
        'id': 'id',
        'name': 'name',
      }, '1');
      database.execute(
        'ALTER TABLE ndjson_import_jobs ADD COLUMN '
        "mapping_sha256 TEXT NOT NULL DEFAULT '$legacy'",
      );
    }
    database.execute(
      '''
      INSERT INTO ndjson_import_jobs
        (job_id, source_sha256, mapping_sha256, next_offset, rows_committed)
      VALUES (?, ?, ?, 0, 0)
      ON CONFLICT(job_id) DO NOTHING
    ''',
      [jobId, digest, mapping],
    );
    final saved = _checkpoint(database, jobId);
    database.execute('COMMIT');
    return saved;
  } catch (_) {
    if (!database.autocommit) database.execute('ROLLBACK');
    rethrow;
  }
}

Row _checkpoint(Database database, String jobId) => database.select(
  'SELECT source_sha256, mapping_sha256, next_offset, rows_committed '
  'FROM ndjson_import_jobs WHERE job_id = ?',
  [jobId],
).single;

Stream<_Line> _readLines(
  File file,
  int offset,
  int lineNumber,
  Iterable<String> keys,
) async* {
  final line = BytesBuilder(copy: false);
  var lineStart = offset;
  await for (final chunk in file.openRead(offset)) {
    var start = 0;
    for (var end = 0; end < chunk.length; end++) {
      if (chunk[end] != 0x0a) continue;
      _append(line, chunk, start, end, lineNumber, lineStart);
      final nextOffset = lineStart + line.length + 1;
      yield (
        values: _decodeRow(line.takeBytes(), lineNumber, lineStart, keys),
        nextOffset: nextOffset,
      );
      lineStart = nextOffset;
      lineNumber++;
      start = end + 1;
    }
    _append(line, chunk, start, chunk.length, lineNumber, lineStart);
  }
  if (line.isNotEmpty) {
    throw FormatException(
      'Missing line ending at line $lineNumber, byte $lineStart.',
    );
  }
}

void _append(
  BytesBuilder line,
  List<int> chunk,
  int start,
  int end,
  int lineNumber,
  int offset,
) {
  if (line.length + end - start > _maxLineBytes) {
    throw FormatException(
      'Line $lineNumber exceeds the byte limit at byte $offset.',
    );
  }
  if (start < end) line.add(chunk.sublist(start, end));
}

List<Object?> _decodeRow(
  Uint8List bytes,
  int lineNumber,
  int offset,
  Iterable<String> keys,
) {
  final content = bytes.isNotEmpty && bytes.last == 0x0d
      ? Uint8List.sublistView(bytes, 0, bytes.length - 1)
      : bytes;
  final Object? value;
  try {
    if (content.contains(0x0d)) throw const FormatException();
    value = jsonDecode(utf8.decode(content));
  } on FormatException {
    throw FormatException('Invalid NDJSON at line $lineNumber, byte $offset.');
  }
  if (value is! Map<String, dynamic>) {
    throw FormatException(
      'Expected a JSON object at line $lineNumber, byte $offset.',
    );
  }
  final values = <Object?>[];
  for (final key in keys) {
    if (!value.containsKey(key)) {
      throw FormatException(
        'Missing a mapped key at line $lineNumber, byte $offset.',
      );
    }
    final field = value[key];
    values.add(switch (field) {
      bool flag => flag ? 1 : 0,
      String() || num() || null => field,
      _ => throw FormatException(
        'Expected a scalar value at line $lineNumber, byte $offset.',
      ),
    });
  }
  return values;
}
