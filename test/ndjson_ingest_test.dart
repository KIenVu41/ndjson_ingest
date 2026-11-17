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
