import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:ndjson_ingest/ndjson_ingest.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

void main() {
  late Directory directory;
  late File source;
  late String target;
  const mapping = {'sku': 'item_id', 'title': 'label', 'active': 'enabled'};

  setUp(() {
    directory = Directory.systemTemp.createTempSync('ndjson-ingest-');
    source = File('${directory.path}/data.ndjson');
    target = '${directory.path}/inventory.sqlite';
    _createInventory(target);
  });
  tearDown(() => directory.deleteSync(recursive: true));

  Future<ImportProgress> ingest({
    String? database,
    Map<String, String> columns = mapping,
    int? maxBatches,
  }) => importNdjson(
    sourcePath: source.path,
    databasePath: database ?? target,
    jobId: 'catalog',
    table: 'inventory',
    columns: columns,
    maxBatches: maxBatches,
  );

  test('redacts SQLite errors and retains the rejected row position', () async {
    const payload = 'SYNTHETIC_PRIVATE_ROW_VALUE';
    const sqliteMessage = 'SYNTHETIC_PRIVATE_SQLITE_MESSAGE';
    _writeSource(source, 503);
    source.writeAsStringSync(
      source.readAsStringSync().replaceFirst('Cà phê 501', payload),
    );
    final bytes = source.readAsBytesSync();
    var rejectedOffset = 0;
    for (var line = 0; line < 501; line++) {
      rejectedOffset = bytes.indexOf(0x0a, rejectedOffset) + 1;
    }
    await ingest(maxBatches: 1);

    final database = sqlite3.open(target);
    try {
      database.execute('''
        CREATE TRIGGER reject_row BEFORE INSERT ON inventory
        WHEN NEW.item_id = 'sku-501'
        BEGIN SELECT RAISE(ABORT, '$sqliteMessage'); END
      ''');
    } finally {
      database.close();
    }

    await expectLater(
      ingest(),
      throwsA(
        isA<NdjsonImportException>()
            .having((error) => error.sqliteCode, 'SQLite extended code', 1811)
            .having((error) => error.line, 'source line', 502)
            .having((error) => error.byteOffset, 'byte offset', rejectedOffset)
            .having(
              (error) => error.toString(),
              'public message',
              'SQLite import failed (code 1811) at line 502, byte $rejectedOffset.',
            ),
      ),
    );
    expect(_read(target, 'SELECT count(*) AS count FROM inventory'), [
      {'count': 500},
    ]);
    expect(_read(target, 'SELECT rows_committed FROM ndjson_import_jobs'), [
      {'rows_committed': 500},
    ]);
  });

  test('redacts the source path from public I/O errors', () async {
    await expectLater(
      ingest(),
      throwsA(
        isA<NdjsonImportException>()
            .having((error) => error.osErrorCode, 'OS code', isNotNull)
            .having(
              (error) => error.toString(),
              'public message',
              'Import file operation failed.',
            ),
      ),
    );
  });

  test(
    'resumes a killed process on a caller-owned schema',
    () async {
      _writeSource(source, 1201);
      final build = '${directory.path}/worker-build';
      final compiled = await Process.run(Platform.resolvedExecutable, [
        'build',
        'cli',
        '--target=test/import_worker.dart',
        '--output=$build',
      ]);
      expect(
        compiled.exitCode,
        0,
        reason: '${compiled.stdout}\n${compiled.stderr}',
      );
      final child = await Process.start('$build/bundle/bin/import_worker', [
        source.path,
        target,
      ]);
      final stdout = child.stdout.drain<void>();
      final marker = RegExp(r'NDJSON_COMMITTED (\d+)');
      try {
        final ready = await utf8.decoder
            .bind(child.stderr)
            .transform(const LineSplitter())
            .firstWhere(marker.hasMatch)
            .timeout(const Duration(seconds: 20));
        final match = marker.firstMatch(ready)!;
        expect(match[1], '500');
        expect(child.kill(ProcessSignal.sigkill), isTrue);
        expect(await child.exitCode, isNot(0));
        expect(
          _read(
            target,
            'SELECT count(*) AS count FROM inventory',
          ).single['count'],
          500,
        );

        final result = await ingest();
        expect(result, (
          rows: 1201,
          offset: source.lengthSync(),
          complete: true,
        ));
        final uninterrupted = '${directory.path}/uninterrupted.sqlite';
        _createInventory(uninterrupted);
        await ingest(database: uninterrupted);
        const query = 'SELECT * FROM inventory ORDER BY item_id';
        expect(_read(target, query), _read(uninterrupted, query));
        expect(await ingest(), result);

        final interrupted = '${directory.path}/interrupted.sqlite';
        _createInventory(interrupted);
        final firstBatch = await ingest(database: interrupted, maxBatches: 1);
        final committedRows = _read(interrupted, query);
        expect(committedRows, hasLength(500));
        final database = sqlite3.open(interrupted);
        try {
          database.execute('PRAGMA journal_mode = DELETE');
          // The preceding INSERT modifies a page. This trigger holds the
          // transaction open on the next row while the parent observes its journal.
          database.execute('''
          CREATE TRIGGER hold_second_row BEFORE INSERT ON inventory
          WHEN NEW.item_id = 'sku-501'
          BEGIN
            SELECT sum(length(randomblob(1024))) FROM (
              WITH RECURSIVE work(n) AS (
                VALUES(1) UNION ALL SELECT n + 1 FROM work WHERE n < 100000000
              )
              SELECT n FROM work
            );
          END
        ''');
        } finally {
          database.close();
        }
        final pending = await Process.start('$build/bundle/bin/import_worker', [
          source.path,
          interrupted,
        ]);
        final pendingOutput = pending.stdout.drain<void>();
        final pendingErrors = pending.stderr.drain<void>();
        try {
          final deadline = Stopwatch()..start();
          final journal = File('$interrupted-journal');
          while (!journal.existsSync() || journal.lengthSync() <= 512) {
            if (deadline.elapsed > const Duration(seconds: 15)) {
              fail('The importer did not open a write transaction.');
            }
            await Future<void>.delayed(const Duration(milliseconds: 10));
          }
          expect(pending.kill(ProcessSignal.sigkill), isTrue);
          expect(await pending.exitCode, isNot(0));
          expect(_read(interrupted, query), committedRows);
          expect(
            _read(interrupted, '''
          SELECT next_offset, rows_committed FROM ndjson_import_jobs
        '''),
            [
              {'next_offset': firstBatch.offset, 'rows_committed': 500},
            ],
          );
          final recovered = sqlite3.open(interrupted);
          try {
            recovered.execute('DROP TRIGGER hold_second_row');
          } finally {
            recovered.close();
          }
          expect(await ingest(database: interrupted), result);
          expect(_read(interrupted, query), _read(uninterrupted, query));
        } finally {
          pending.kill();
          await pending.stdin.close();
          await pending.exitCode;
          await pendingOutput;
          await pendingErrors;
        }
      } finally {
        child.kill();
        await child.stdin.close();
        await child.exitCode;
        await stdout;
      }
    },
    skip: Platform.isWindows ? 'Requires POSIX process signals.' : false,
    timeout: const Timeout(Duration(minutes: 2)),
  );

  test(
    'refuses changed input and mapping before writing another batch',
    () async {
      _writeSource(source, 501);
      expect((await ingest(maxBatches: 1)).rows, 500);
      await expectLater(
        ingest(columns: {'sku': 'item_id', 'title': 'label'}),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            'message',
            contains('mapping changed'),
          ),
        ),
      );
      final original = source.readAsStringSync();
      source.writeAsStringSync(
        original.replaceFirst('Cà phê 500', 'Cà phê 999'),
      );
      await expectLater(
        ingest(),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            'message',
            contains('source changed'),
          ),
        ),
      );
      expect(
        _read(
          target,
          'SELECT count(*) AS count FROM inventory',
        ).single['count'],
        500,
      );
      source.writeAsStringSync(original);
      expect(
        (await ingest(
          columns: {'active': 'enabled', 'title': 'label', 'sku': 'item_id'},
        )).rows,
        501,
      );
    },
  );

  test('concurrent workers cannot duplicate a committed batch', () async {
    _writeSource(source, 1501);
    await Future.wait(
      List.generate(2, (_) async {
        try {
          await ingest();
        } on StateError catch (error) {
          expect(error.message, contains('Another worker changed'));
        }
      }),
    );
    expect((await ingest()).rows, 1501);
    expect(
      _read(target, 'SELECT count(*) AS count FROM inventory').single['count'],
      1501,
    );
  });

  test('resumes a checkpoint written by the original prototype', () async {
    source.writeAsStringSync('{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n');
    final database = sqlite3.open(target);
    try {
      database.execute(
        'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
      );
      database.execute("INSERT INTO products VALUES (1, 'a')");
      database.execute('''
        CREATE TABLE ndjson_import_jobs (
          job_id TEXT PRIMARY KEY NOT NULL,
          source_sha256 TEXT NOT NULL,
          next_offset INTEGER NOT NULL,
          rows_committed INTEGER NOT NULL
        )
      ''');
      database.execute('INSERT INTO ndjson_import_jobs VALUES (?, ?, ?, 1)', [
        'legacy',
        sha256.convert(source.readAsBytesSync()).toString(),
        utf8.encode('{"id":1,"name":"a"}\n').length,
      ]);
    } finally {
      database.close();
    }
    final result = await importNdjson(
      sourcePath: source.path,
      databasePath: target,
      jobId: 'legacy',
      table: 'products',
      columns: const {'id': 'id', 'name': 'name'},
    );
    expect(result.rows, 2);
    expect(_read(target, 'SELECT name FROM products ORDER BY id'), [
      {'name': 'a'},
      {'name': 'b'},
    ]);
  });
}

void _createInventory(String path) {
  final database = sqlite3.open(path);
  try {
    database.execute('''
      CREATE TABLE inventory (
        item_id TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        enabled INTEGER NOT NULL CHECK(enabled IN (0, 1))
      )
    ''');
  } finally {
    database.close();
  }
}

List<Map<String, Object?>> _read(String path, String sql) {
  final database = sqlite3.open(path);
  try {
    return database.select(sql).map(Map<String, Object?>.of).toList();
  } finally {
    database.close();
  }
}

void _writeSource(File path, int rows) {
  path.writeAsStringSync(
    List.generate(
      rows,
      (i) =>
          '${jsonEncode({'sku': 'sku-$i', 'title': 'Cà phê $i', 'active': i.isEven})}${i.isEven ? '\n' : '\r\n'}',
    ).join(),
  );
}
