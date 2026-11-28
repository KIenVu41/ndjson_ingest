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

/// A cooperative cancellation request for one import invocation.
///
/// Create and cancel the token in the isolate that calls [importNdjson].
final class ImportCancellationToken {
  /// Creates an unused token. Create a new token for each import invocation.
  ImportCancellationToken();

  bool _used = false;
  bool _cancelled = false;

  /// Requests cancellation. Repeated calls are harmless.
  ///
  /// This does not wait for cleanup or interrupt a SQLite transaction. Await
  /// the import future before deleting files or starting a replacement job.
  void cancel() => _cancelled = true;
}

/// An import stopped cooperatively after releasing its resources.
final class NdjsonImportCancelled implements Exception {
  const NdjsonImportCancelled._([this.progress]);

  /// The last verified, committed checkpoint, or null before verification.
  final ImportProgress? progress;

  @override
  String toString() => 'NDJSON import cancelled.';
}

typedef _Line = ({List<Object?> values, int offset, int nextOffset});

/// Stable categories for sanitized import failures.
enum NdjsonImportErrorCode {
  sqlite,
  io,
  invalidJson,
  expectedObject,
  missingMappedKey,
  unsupportedValue,
  lineTooLong,
  missingLineEnding,
  rowExceedsBatchLimit,
}

/// An import failure with details safe to expose to application logs.
///
/// The original exception, SQL, bound values, and file paths are omitted.
final class NdjsonImportException implements Exception {
  const NdjsonImportException._({
    required this.code,
    this.sqliteCode,
    this.osErrorCode,
    this.line,
    this.byteOffset,
  });

  /// The machine-readable failure category.
  final NdjsonImportErrorCode code;

  /// SQLite's extended result code, when the failure came from SQLite.
  final int? sqliteCode;

  /// The operating-system error code, when available for an I/O failure.
  final int? osErrorCode;

  /// The one-based source line, when a specific row was rejected.
  final int? line;

  /// The zero-based byte offset of [line], when available.
  final int? byteOffset;

  @override
  String toString() {
    final message = switch (code) {
      NdjsonImportErrorCode.sqlite => 'SQLite import failed (code $sqliteCode)',
      NdjsonImportErrorCode.io => 'Import file operation failed',
      _ => 'NDJSON import failed (${code.name})',
    };
    final position = line == null ? '' : ' at line $line, byte $byteOffset';
    return '$message$position.';
  }
}

/// Imports an immutable local NDJSON file into an existing SQLite table.
///
/// [columns] maps top-level JSON keys to SQLite column names. Mapped values
/// must be strings, numbers, booleans, or null. Booleans are bound as 0 or 1.
/// Every mapped key must exist in each row. SQLite enforces table constraints.
///
/// Use the same [jobId] and configuration to resume. Change [mappingVersion]
/// when the target schema changes the meaning of imported data. [maxBatches]
/// can pause the import at a committed boundary. The result is cumulative.
///
/// [batchRows] and [maxBatchBytes] bound each transaction. Batch bytes include
/// line endings. A single row exceeding the batch limit is rejected.
/// [maxLineBytes] counts bytes before LF, including CR for CRLF.
/// These limits may change when resuming without changing the mapping.
///
/// [onProgress] receives committed snapshots in the calling isolate after source
/// verification, at most once per 100 ms during import, and at the end. Initial
/// and final updates bypass throttling. Intermediate batches may be omitted.
/// The last snapshot matches the result on success, including [maxBatches] pauses.
/// Callbacks are synchronous; returned futures are not awaited. If a callback
/// throws, further callbacks stop while the worker finishes and closes SQLite.
/// Its first error is then rethrown unless the worker itself failed, in which
/// case the worker error takes precedence. Callback errors do not cancel or
/// roll back the import. No callbacks occur after the returned future completes.
///
/// [cancellationToken] requests cooperative stopping while hashing, reading or
/// between transactions. A running transaction finishes or rolls back first.
/// Await the returned future before disposing resources. Cancellation throws
/// [NdjsonImportCancelled] with the last verified checkpoint, or null before
/// verification. An already committed EOF returns a completed result instead.
/// Each token is single-use; callback errors alone still do not cancel imports.
///
/// NDJSON, SQLite and file I/O failures are reported as [NdjsonImportException].
Future<ImportProgress> importNdjson({
  required String sourcePath,
  required String databasePath,
  required String jobId,
  required String table,
  required Map<String, String> columns,
  String mappingVersion = '1',
  int? maxBatches,
  int batchRows = _batchRows,
  int maxBatchBytes = _maxBatchBytes,
  int maxLineBytes = _maxLineBytes,
  void Function(ImportProgress progress)? onProgress,
  ImportCancellationToken? cancellationToken,
}) {
  if (batchRows < 1 || maxBatchBytes < 1 || maxLineBytes < 1) {
    throw ArgumentError('Batch and line limits must be positive.');
  }
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
  return _observeImport(
    onProgress,
    cancellationToken,
    (port) => _startImport(
      sourcePath,
      databasePath,
      jobId,
      table,
      mapping,
      mappingVersion,
      maxBatches,
      batchRows,
      maxBatchBytes,
      maxLineBytes,
      port,
      cancellationToken != null,
      onProgress != null,
    ),
  );
}

Future<ImportProgress> _observeImport(
  void Function(ImportProgress)? onProgress,
  ImportCancellationToken? cancellationToken,
  Future<ImportProgress> Function(SendPort?) start,
) async {
  if (cancellationToken != null) {
    if (cancellationToken._used) {
      throw StateError('A cancellation token can only be used once.');
    }
    cancellationToken._used = true;
    if (cancellationToken._cancelled) throw const NdjsonImportCancelled._();
  }
  if (onProgress == null && cancellationToken == null) return start(null);
  final port = ReceivePort();
  Object? callbackError;
  StackTrace? callbackStack;
  final subscription = port.listen((message) {
    if (message case (cancellationCheck: SendPort reply)) {
      reply.send(cancellationToken!._cancelled);
      return;
    }
    if (message is SendPort) {
      message.send(null);
      return;
    }
    if (callbackError != null || onProgress == null) return;
    try {
      onProgress(message as ImportProgress);
    } catch (error, stack) {
      callbackError = error;
      callbackStack = stack;
    }
  });
  try {
    final result = await start(port.sendPort);
    if (callbackError != null) {
      Error.throwWithStackTrace(callbackError!, callbackStack!);
    }
    return result;
  } finally {
    port.close();
    await subscription.cancel();
  }
}

// Keep the isolate closure outside the caller's scope, which holds the callback.
Future<ImportProgress> _startImport(
  String sourcePath,
  String databasePath,
  String jobId,
  String table,
  Map<String, String> columns,
  String mappingVersion,
  int? maxBatches,
  int batchRows,
  int maxBatchBytes,
  int maxLineBytes,
  SendPort? coordinator,
  bool cancellable,
  bool reportProgress,
) => Isolate.run(() async {
  try {
    return await _import(
      sourcePath,
      databasePath,
      jobId,
      table,
      columns,
      mappingVersion,
      maxBatches,
      batchRows,
      maxBatchBytes,
      maxLineBytes,
      reportProgress ? coordinator : null,
      cancellable ? coordinator : null,
    );
  } on SqliteException catch (error) {
    throw NdjsonImportException._(
      code: NdjsonImportErrorCode.sqlite,
      sqliteCode: error.extendedResultCode,
    );
  } on FileSystemException catch (error) {
    throw NdjsonImportException._(
      code: NdjsonImportErrorCode.io,
      osErrorCode: error.osError?.errorCode,
    );
  } finally {
    if (coordinator != null) {
      final acknowledged = ReceivePort();
      try {
        // Drain updates before the result arrives on Isolate.run's separate port.
        coordinator.send(acknowledged.sendPort);
        await acknowledged.first;
      } finally {
        acknowledged.close();
      }
    }
  }
});

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
  int batchRows,
  int maxBatchBytes,
  int maxLineBytes,
  SendPort? progressPort,
  SendPort? cancellationPort,
) async {
  if (cancellationPort != null) await _cancellationPoint(cancellationPort);
  final file = File(sourcePath);
  final size = await file.length();
  if (await File(databasePath).exists() &&
      await FileSystemEntity.identical(sourcePath, databasePath)) {
    throw StateError('The source and database must be different files.');
  }
  final hashInput = cancellationPort == null
      ? file.openRead()
      : _cancellableHashInput(file, cancellationPort);
  final digest = (await sha256.bind(hashInput).single).toString();
  if (cancellationPort != null) await _cancellationPoint(cancellationPort);
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
      final notifications = Stopwatch()..start();
      ImportProgress? lastProgress;
      void report({bool force = false}) {
        if (progressPort == null) return;
        final progress = (rows: rows, offset: offset, complete: offset == size);
        if (progress == lastProgress) return;
        if (!force && notifications.elapsedMilliseconds < 100) return;
        progressPort.send(progress);
        lastProgress = progress;
        notifications.reset();
      }

      report(force: true);
      if (offset == size) return (rows: rows, offset: offset, complete: true);

      Future<void> checkCancellation() async {
        if (cancellationPort != null && offset != size) {
          await _cancellationPoint(cancellationPort, (
            rows: rows,
            offset: offset,
            complete: false,
          ));
        }
      }

      var batches = 0;
      var pendingOffset = offset;
      final batch = <_Line>[];

      Future<void> commit() async {
        if (cancellationPort != null) await checkCancellation();
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
          for (var index = 0; index < batch.length; index++) {
            final row = batch[index];
            try {
              insert.execute(row.values);
            } on SqliteException catch (error) {
              throw NdjsonImportException._(
                code: NdjsonImportErrorCode.sqlite,
                sqliteCode: error.extendedResultCode,
                line: rows + index + 1,
                byteOffset: row.offset,
              );
            }
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
        report();
        if (cancellationPort != null) await checkCancellation();
      }

      try {
        await for (final line in _readLines(
          file,
          offset,
          rows + 1,
          columns.keys,
          maxLineBytes,
          cancellationPort == null ? null : checkCancellation,
        )) {
          if (line.nextOffset - line.offset > maxBatchBytes) {
            throw NdjsonImportException._(
              code: NdjsonImportErrorCode.rowExceedsBatchLimit,
              line: rows + batch.length + 1,
              byteOffset: line.offset,
            );
          }
          if (batch.isNotEmpty && line.nextOffset - offset > maxBatchBytes) {
            await commit();
            if (batches == maxBatches) break;
          }
          batch.add(line);
          pendingOffset = line.nextOffset;
          if (batch.length == batchRows) {
            await commit();
            if (batches == maxBatches) break;
          }
        }
        if (batch.isNotEmpty) await commit();
        return (rows: rows, offset: offset, complete: offset == size);
      } finally {
        report(force: true);
      }
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
  int maxLineBytes,
  Future<void> Function()? checkCancellation,
) async* {
  final line = BytesBuilder(copy: false);
  var lineStart = offset;
  await for (final chunk in file.openRead(offset)) {
    if (checkCancellation != null) await checkCancellation();
    var start = 0;
    for (var end = 0; end < chunk.length; end++) {
      if (chunk[end] != 0x0a) continue;
      _append(line, chunk, start, end, lineNumber, lineStart, maxLineBytes);
      final nextOffset = lineStart + line.length + 1;
      yield (
        values: _decodeRow(line.takeBytes(), lineNumber, lineStart, keys),
        offset: lineStart,
        nextOffset: nextOffset,
      );
      lineStart = nextOffset;
      lineNumber++;
      start = end + 1;
    }
    _append(
      line,
      chunk,
      start,
      chunk.length,
      lineNumber,
      lineStart,
      maxLineBytes,
    );
  }
  if (checkCancellation != null) await checkCancellation();
  if (line.isNotEmpty) {
    throw NdjsonImportException._(
      code: NdjsonImportErrorCode.missingLineEnding,
      line: lineNumber,
      byteOffset: lineStart,
    );
  }
}

Future<void> _cancellationPoint(
  SendPort coordinator, [
  ImportProgress? progress,
]) async {
  final reply = ReceivePort();
  try {
    // The same channel orders this check after preceding progress callbacks.
    coordinator.send((cancellationCheck: reply.sendPort));
    if (await reply.first as bool) throw NdjsonImportCancelled._(progress);
  } finally {
    reply.close();
  }
}

Stream<List<int>> _cancellableHashInput(
  File file,
  SendPort coordinator,
) async* {
  await for (final chunk in file.openRead()) {
    await _cancellationPoint(coordinator);
    yield chunk;
  }
}

void _append(
  BytesBuilder line,
  List<int> chunk,
  int start,
  int end,
  int lineNumber,
  int offset,
  int maxLineBytes,
) {
  if (line.length + end - start > maxLineBytes) {
    throw NdjsonImportException._(
      code: NdjsonImportErrorCode.lineTooLong,
      line: lineNumber,
      byteOffset: offset,
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
    throw NdjsonImportException._(
      code: NdjsonImportErrorCode.invalidJson,
      line: lineNumber,
      byteOffset: offset,
    );
  }
  if (value is! Map<String, dynamic>) {
    throw NdjsonImportException._(
      code: NdjsonImportErrorCode.expectedObject,
      line: lineNumber,
      byteOffset: offset,
    );
  }
  final values = <Object?>[];
  for (final key in keys) {
    if (!value.containsKey(key)) {
      throw NdjsonImportException._(
        code: NdjsonImportErrorCode.missingMappedKey,
        line: lineNumber,
        byteOffset: offset,
      );
    }
    final field = value[key];
    values.add(switch (field) {
      bool flag => flag ? 1 : 0,
      String() || num() || null => field,
      _ => throw NdjsonImportException._(
        code: NdjsonImportErrorCode.unsupportedValue,
        line: lineNumber,
        byteOffset: offset,
      ),
    });
  }
  return values;
}
