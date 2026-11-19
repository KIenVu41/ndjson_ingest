import 'dart:convert';
import 'dart:io';

import 'package:ndjson_ingest/ndjson_ingest.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

void main() {
  late Directory temp;
  late File source;
  late String target;
  setUp(() {
    temp = Directory.systemTemp.createTempSync('ndjson-limits-');
    source = File('${temp.path}/source.ndjson');
    target = '${temp.path}/target.sqlite';
    final db = sqlite3.open(target);
    try {
      db.execute('CREATE TABLE records (value TEXT)');
    } finally {
      db.close();
    }
  });
  tearDown(() => temp.deleteSync(recursive: true));

  Future<ImportProgress> run({
    int batchRows = 500,
    int maxBatchBytes = 4 * 1024 * 1024,
    int maxLineBytes = 256 * 1024,
    int? maxBatches,
  }) => importNdjson(
    sourcePath: source.path,
    databasePath: target,
    jobId: 'limits',
    table: 'records',
    columns: const {'value': 'value'},
    batchRows: batchRows,
    maxBatchBytes: maxBatchBytes,
    maxLineBytes: maxLineBytes,
    maxBatches: maxBatches,
  );

  void checkRows(int expected) {
    final db = sqlite3.open(target);
    try {
      expect(
        db.select('SELECT count(*) AS n FROM records').single['n'],
        expected,
      );
      expect(
        db
            .select('SELECT rows_committed FROM ndjson_import_jobs')
            .single['rows_committed'],
        expected,
      );
    } finally {
      db.close();
    }
  }

  test('validates positive limits before starting the worker', () {
    expect(() => run(batchRows: 0), throwsArgumentError);
    expect(() => run(maxBatchBytes: -1), throwsArgumentError);
    expect(() => run(maxLineBytes: 0), throwsArgumentError);
    expect(File(target).lengthSync(), greaterThan(0));
  });

  test(
    'byte boundary and resume with different limits preserve all rows',
    () async {
      const row = '{"value":"é"}\r\n';
      final length = utf8.encode(row).length;
      source.writeAsStringSync(List.filled(7, row).join());
      final first = await run(maxBatchBytes: length * 2, maxBatches: 1);
      expect(first, (rows: 2, offset: length * 2, complete: false));
      checkRows(2);
      final second = await run(batchRows: 3, maxBatchBytes: length * 3);
      expect(second, (rows: 7, offset: length * 7, complete: true));
      checkRows(7);
      expect(await run(batchRows: 1), second);
    },
  );

  test('line limit includes CR but excludes LF', () async {
    const row = '{"value":"é"}\r\n';
    final length = utf8.encode(row).length;
    source.writeAsStringSync(row);
    await expectLater(
      run(maxLineBytes: length - 2),
      throwsA(
        isA<NdjsonImportException>().having(
          (e) => e.code,
          'code',
          NdjsonImportErrorCode.lineTooLong,
        ),
      ),
    );
    checkRows(0);
    expect((await run(maxLineBytes: length - 1)).rows, 1);
  });

  test('a row cannot silently exceed the batch byte limit', () async {
    const row = '{"value":"hello"}\n';
    source.writeAsStringSync(row);
    await expectLater(
      run(maxBatchBytes: utf8.encode(row).length - 1),
      throwsA(
        isA<NdjsonImportException>()
            .having(
              (e) => e.code,
              'code',
              NdjsonImportErrorCode.rowExceedsBatchLimit,
            )
            .having((e) => e.line, 'line', 1)
            .having((e) => e.byteOffset, 'offset', 0),
      ),
    );
    checkRows(0);
  });

  const cases = {
    '{"value":"PRIVATE_PAYLOAD",': NdjsonImportErrorCode.invalidJson,
    '[]\n': NdjsonImportErrorCode.expectedObject,
    '{"PRIVATE_KEY":"PRIVATE_PAYLOAD"}\n':
        NdjsonImportErrorCode.missingMappedKey,
    '{"value":["PRIVATE_PAYLOAD"]}\n': NdjsonImportErrorCode.unsupportedValue,
    '{"value":"PRIVATE_PAYLOAD"}': NdjsonImportErrorCode.missingLineEnding,
    '\n': NdjsonImportErrorCode.invalidJson,
  };
  for (final entry in cases.entries) {
    // Invalid JSON must still have a line ending to reach JSON decoding.
    final invalid =
        entry.value == NdjsonImportErrorCode.invalidJson && entry.key != '\n'
        ? '${entry.key}\n'
        : entry.key;
    test(
      'sanitizes ${entry.value.name} and retains the previous checkpoint',
      () async {
        const good = '{"value":"valid"}\n';
        source.writeAsStringSync('$good$good$good$invalid');
        await expectLater(
          run(batchRows: 2),
          throwsA(
            isA<NdjsonImportException>()
                .having((e) => e.code, 'code', entry.value)
                .having((e) => e.line, 'line', 4)
                .having((e) => e.byteOffset, 'offset', good.length * 3)
                .having(
                  (e) => e.toString(),
                  'sanitized message',
                  allOf(isNot(contains('PRIVATE')), isNot(contains(temp.path))),
                ),
          ),
        );
        checkRows(2);
      },
    );
  }

  test('large Unicode and CRLF records cross stream chunks intact', () async {
    final value = List.filled(70000, 'é').join();
    source.writeAsStringSync('${jsonEncode({'value': value})}\r\n');
    expect((await run()).rows, 1);
    final db = sqlite3.open(target);
    try {
      expect(db.select('SELECT value FROM records').single['value'], value);
    } finally {
      db.close();
    }
  });
}
