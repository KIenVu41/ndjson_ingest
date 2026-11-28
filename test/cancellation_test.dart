import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:ndjson_ingest/ndjson_ingest.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

void main() {
  late Directory directory;
  late File source;
  late String target;
  setUp(() {
    directory = Directory.systemTemp.createTempSync('ndjson-cancellation-');
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
    ImportCancellationToken? token,
    int batchRows = 2,
    int? maxBatches,
    void Function(ImportProgress)? onProgress,
  }) => importNdjson(
    sourcePath: source.path,
    databasePath: target,
    jobId: 'cancel',
    table: 'records',
    columns: const {'id': 'id', 'value': 'value'},
    batchRows: batchRows,
    maxBatches: maxBatches,
    cancellationToken: token,
    onProgress: onProgress,
  );

  void checkCheckpoint(ImportProgress progress) {
    final db = sqlite3.open(target);
    try {
      final saved = db
          .select('SELECT rows_committed, next_offset FROM ndjson_import_jobs')
          .single;
      expect(saved['rows_committed'], progress.rows);
      expect(saved['next_offset'], progress.offset);
      expect(
        db.select('SELECT count(*) AS n FROM records').single['n'],
        progress.rows,
      );
    } finally {
      db.close();
    }
  }

  void holdTransaction({bool reject = false}) {
    final db = sqlite3.open(target);
    try {
      db.execute('PRAGMA journal_mode = DELETE');
      db.execute('''
        CREATE TRIGGER hold_second BEFORE INSERT ON records
        WHEN NEW.id = 1 BEGIN
          SELECT sum(n) FROM (
            WITH RECURSIVE work(n) AS (
              VALUES(1) UNION ALL SELECT n+1 FROM work WHERE n < 3000000
            ) SELECT n FROM work
          );
          ${reject ? "SELECT RAISE(ABORT, 'PRIVATE_TRIGGER_MESSAGE');" : ''}
        END
      ''');
    } finally {
      db.close();
    }
  }

  Future<void> waitForTransaction() async {
    final journal = File('$target-journal');
    final watch = Stopwatch()..start();
    while (!journal.existsSync() || journal.lengthSync() <= 512) {
      if (watch.elapsed > const Duration(seconds: 15)) {
        fail('Importer did not open a write transaction.');
      }
      await Future<void>.delayed(const Duration(milliseconds: 2));
    }
  }

  test('pre-cancelled tokens do no source I/O and emit no snapshots', () async {
    final token = ImportCancellationToken()
      ..cancel()
      ..cancel();
    final snapshots = <ImportProgress>[];
    await expectLater(
      run(token: token, onProgress: snapshots.add),
      throwsA(
        isA<NdjsonImportCancelled>()
            .having((e) => e.progress, 'progress', isNull)
            .having((e) => e.toString(), 'message', 'NDJSON import cancelled.'),
      ),
    );
    expect(source.existsSync(), isFalse);
    expect(snapshots, isEmpty);
    final db = sqlite3.open(target);
    try {
      expect(
        db.select(
          "SELECT name FROM sqlite_master WHERE name='ndjson_import_jobs'",
        ),
        isEmpty,
      );
    } finally {
      db.close();
    }
  });

  test(
    'cancellation immediately after starting works without a callback',
    () async {
      final token = ImportCancellationToken();
      final future = run(token: token);
      token.cancel();
      await expectLater(
        future,
        throwsA(
          isA<NdjsonImportCancelled>().having(
            (e) => e.progress,
            'progress',
            isNull,
          ),
        ),
      );
    },
  );

  test(
    'stopping before verification cancels hashing without creating a job',
    () async {
      final file = source.openSync(mode: FileMode.write);
      try {
        // Sparse source: a long hash without allocating a large disk fixture.
        file.truncateSync(128 * 1024 * 1024);
      } finally {
        file.closeSync();
      }
      final token = ImportCancellationToken();
      final timer = Timer(const Duration(milliseconds: 10), token.cancel);
      try {
        await expectLater(
          run(token: token),
          throwsA(
            isA<NdjsonImportCancelled>().having(
              (e) => e.progress,
              'progress',
              isNull,
            ),
          ),
        );
      } finally {
        timer.cancel();
      }
      final db = sqlite3.open(target);
      try {
        expect(
          db.select(
            "SELECT name FROM sqlite_master WHERE name='ndjson_import_jobs'",
          ),
          isEmpty,
        );
      } finally {
        db.close();
      }
    },
  );

  test(
    'an initial callback can cancel before any rows are committed',
    () async {
      writeRows(7);
      final token = ImportCancellationToken();
      final snapshots = <ImportProgress>[];
      await expectLater(
        run(
          token: token,
          onProgress: (progress) {
            snapshots.add(progress);
            token.cancel();
          },
        ),
        throwsA(
          isA<NdjsonImportCancelled>().having((e) => e.progress, 'progress', (
            rows: 0,
            offset: 0,
            complete: false,
          )),
        ),
      );
      expect(snapshots, [(rows: 0, offset: 0, complete: false)]);
      checkCheckpoint(snapshots.single);
      expect((await run()).rows, 7);
      final count = snapshots.length;
      await Future<void>.delayed(Duration.zero);
      expect(snapshots.length, count);
    },
  );

  test('cancelling a resumed job preserves its verified checkpoint', () async {
    writeRows(7);
    final paused = await run(maxBatches: 1);
    final token = ImportCancellationToken();
    final snapshots = <ImportProgress>[];
    await expectLater(
      run(
        token: token,
        onProgress: (progress) {
          snapshots.add(progress);
          token.cancel();
        },
      ),
      throwsA(
        isA<NdjsonImportCancelled>().having(
          (e) => e.progress,
          'progress',
          paused,
        ),
      ),
    );
    expect(snapshots, [paused]);
    checkCheckpoint(paused);
    expect((await run()).rows, 7);
  });

  test(
    'a token is single-use, including concurrent and completed calls',
    () async {
      writeRows(7);
      final token = ImportCancellationToken();
      final active = run(token: token);
      await expectLater(run(token: token), throwsStateError);
      final completed = await active;
      expect(completed.complete, isTrue);
      token.cancel();
      token.cancel();
      await expectLater(run(token: token), throwsStateError);
      checkCheckpoint(completed);
    },
  );

  test(
    'callback errors alone still do not cancel a cancellable worker',
    () async {
      writeRows(7);
      final failure = StateError('Observer failed');
      await expectLater(
        run(
          token: ImportCancellationToken(),
          onProgress: (_) {
            throw failure;
          },
        ),
        throwsA(same(failure)),
      );
      expect((await run()).rows, 7);
    },
  );

  test(
    'an acknowledged cancellation takes precedence over callback errors',
    () async {
      writeRows(7);
      final token = ImportCancellationToken();
      await expectLater(
        run(
          token: token,
          onProgress: (_) {
            token.cancel();
            throw StateError('Observer failed');
          },
        ),
        throwsA(
          isA<NdjsonImportCancelled>().having((e) => e.progress, 'progress', (
            rows: 0,
            offset: 0,
            complete: false,
          )),
        ),
      );
      expect((await run()).rows, 7);
    },
  );

  for (final reject in [false, true]) {
    test(
      'cancellation during a transaction preserves ${reject ? 'SQL errors' : 'its committed boundary'}',
      () async {
        writeRows(7);
        holdTransaction(reject: reject);
        final token = ImportCancellationToken();
        final ready = Completer<void>();
        final snapshots = <ImportProgress>[];
        final future = run(
          token: token,
          onProgress: (progress) {
            snapshots.add(progress);
            if (!ready.isCompleted) ready.complete();
          },
        );
        final expectation = expectLater(
          future,
          throwsA(
            reject
                ? isA<NdjsonImportException>()
                      .having((e) => e.sqliteCode, 'code', 1811)
                      .having(
                        (e) => e.toString(),
                        'redacted',
                        isNot(contains('PRIVATE_TRIGGER_MESSAGE')),
                      )
                : isA<NdjsonImportCancelled>().having(
                    (e) => e.progress?.rows,
                    'rows',
                    2,
                  ),
          ),
        );
        try {
          await ready.future;
          await waitForTransaction();
          token.cancel();
          await expectation;
        } finally {
          token.cancel();
          try {
            await future;
          } catch (_) {}
        }
        expect(snapshots.last.rows, reject ? 0 : 2);
        checkCheckpoint(snapshots.last);
        final db = sqlite3.open(target);
        try {
          db.execute('DROP TRIGGER hold_second');
        } finally {
          db.close();
        }
        expect((await run()).rows, 7);
      },
    );
  }

  test(
    'committing EOF wins over a cancellation queued during the final transaction',
    () async {
      writeRows(2);
      holdTransaction();
      final token = ImportCancellationToken();
      final ready = Completer<void>();
      final future = run(
        token: token,
        onProgress: (_) {
          if (!ready.isCompleted) ready.complete();
        },
      );
      try {
        await ready.future;
        await waitForTransaction();
        token.cancel();
        final result = await future;
        expect(result, (rows: 2, offset: source.lengthSync(), complete: true));
        checkCheckpoint(result);
      } finally {
        token.cancel();
        try {
          await future;
        } catch (_) {}
      }
    },
  );

  test(
    'already completed jobs stay complete when their final callback cancels',
    () async {
      writeRows(2);
      final completed = await run();
      final token = ImportCancellationToken();
      expect(
        await run(token: token, onProgress: (_) => token.cancel()),
        completed,
      );
    },
  );

  test(
    'maxBatches pauses normally when a token has no cancellation request',
    () async {
      writeRows(7);
      final token = ImportCancellationToken();
      final result = await run(token: token, maxBatches: 1);
      expect(result.rows, 2);
      expect(result.complete, isFalse);
      checkCheckpoint(result);
    },
  );
}
