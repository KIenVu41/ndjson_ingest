# ndjson_ingest

Import local NDJSON files into SQLite in a worker isolate, with bounded
batches and atomic checkpoints for resuming after the process stops.
Version 0.1.0 supports local file imports with the Dart VM on Linux and macOS.

## Installation

Requires Dart **3.10.0 or later in the 3.x series**.

```sh
dart pub add ndjson_ingest
```

To use the SQLite table-creation code below, also add its direct dependency:

```sh
dart pub add sqlite3
```

The package uses `sqlite3` native build hooks to supply SQLite. The destination
file and table must exist before importing. The library does not create your
application schema.

## Supported environments

| Environment | Support in 0.1.0 | Verification |
| --- | --- | --- |
| Dart VM on Linux | Supported | Ubuntu CI on Dart 3.10.0 and 3.13.4 |
| Dart VM on macOS | Supported | Local macOS arm64 checks; macOS CI configured on Dart 3.13.4 |
| Dart VM on Windows | Not yet declared supported | Not verified |
| Flutter desktop, Android and iOS | Not yet declared supported | Application builds and device imports not verified |
| Web | Unsupported | Requires native file I/O, isolates and SQLite |

The platform metadata declares only Linux and macOS. Flutter compatibility is
not implied by Dart VM tests. Support for additional environments will be
announced after application-level verification.

## Development

The repository pins Flutter 3.47.5 through FVM to provide Dart 3.13.4 for
maintainers. This FVM version is not a requirement for package consumers.

```sh
fvm dart pub get
fvm dart analyze
fvm dart test
fvm dart run tool/check_sqlite.dart
```

The SQLite check opens an in-memory database using the native library supplied
by the SQLite build hook. It prints the engine version and verifies a bound
insert and read. GitHub Actions runs the test suite on Ubuntu for both SDKs
and has a separate macOS job. Ubuntu check names are unchanged for existing
branch protection rules.

## CLI example in the repository

The CLI accepts UTF-8 NDJSON objects with an integer `id` and a string
`name`. It writes them to the `products` table of a dedicated SQLite
database. Use a separate source file and database file.

```sh
printf '%s\n' '{"id":1,"name":"Cà phê"}' > products.ndjson
fvm dart run tool/import_ndjson.dart products.ndjson products.sqlite catalog-v1
```

The optional fourth argument limits how many batches to commit in one run.
Run the command again with the same job ID and file to resume. The CLI prints
the cumulative committed row count, next byte offset, and whether the job
reached the end. Keep the source immutable during import. Each run streams a
SHA-256 hash of the entire source before it resumes. This uses bounded memory
but requires an O(file size) read even when the checkpoint is near the end.

Every line needs an LF or CRLF ending. The importer rejects blank lines and
limits a line to 256 KiB and a batch to 500 rows or 4 MiB of source bytes,
whichever comes first. A single worker isolate commits rows and the checkpoint
in one transaction. Duplicate IDs or an interrupted transaction roll back the
current batch. Earlier committed batches remain available.

## Import into your own table

This complete example creates a temporary source file and database, creates
the destination table, imports one batch, then resumes the remaining rows.
It deletes the temporary files in `finally`, including when an import fails.
For production, use persistent application paths and keep the source immutable.

```dart
import 'dart:convert';
import 'dart:io';

import 'package:ndjson_ingest/ndjson_ingest.dart';
import 'package:sqlite3/sqlite3.dart';

Future<void> main() async {
  final directory = await Directory.systemTemp.createTemp('ndjson-example-');
  try {
    final source = File('${directory.path}/inventory.ndjson');
    final databasePath = '${directory.path}/inventory.sqlite';
    final rows = List.generate(
      501,
      (index) => jsonEncode({'sku': 'SKU-$index', 'title': 'Item $index'}),
    );
    await source.writeAsString('${rows.join('\n')}\n');

    final database = sqlite3.open(databasePath);
    try {
      database.execute('''
        CREATE TABLE inventory (
          item_id TEXT PRIMARY KEY NOT NULL,
          label TEXT NOT NULL
        )
      ''');
    } finally {
      database.close();
    }

    Future<ImportProgress> runImport({int? maxBatches}) => importNdjson(
      sourcePath: source.path,
      databasePath: databasePath,
      jobId: 'inventory-v1',
      table: 'inventory',
      columns: const {'sku': 'item_id', 'title': 'label'},
      maxBatches: maxBatches,
    );

    final paused = await runImport(maxBatches: 1);
    print('Paused after ${paused.rows} committed rows.');

    final resumed = await runImport();
    print(
      'Finished with ${resumed.rows} rows (complete: ${resumed.complete}).',
    );
  } finally {
    await directory.delete(recursive: true);
  }
}
```

The output reports 500 committed rows, followed by 501 total rows. The
[runnable example](example/ndjson_ingest_example.dart) contains the same code.
From a repository checkout, run it with
`fvm dart run example/ndjson_ingest_example.dart`.

Each mapped key must be present. Supported values are strings, numbers,
booleans and null. Booleans bind as SQLite integers, and the destination table
enforces its constraints. Committed batches are visible during the import.

Resume requires the same source content, target table, mapping and
`mappingVersion`. Change `mappingVersion` when a schema or interpretation
change alters what the same mapping means. An incompatible job requires a
new job ID. Existing rows remain in the destination, so the application
must decide how to handle them before importing again.

The defaults are `batchRows: 500`, `maxBatchBytes: 4 * 1024 * 1024`, and
`maxLineBytes: 256 * 1024`. Pass these named arguments to `importNdjson` to
override them. `maxBatches: 1` pauses after one committed batch.

All three limits must be positive. `maxLineBytes` counts the UTF-8 bytes before
LF, including CR for CRLF. `maxBatchBytes` includes line endings and limits
source bytes, not decoded object memory or SQLite's total memory usage. A
single row larger than this limit fails with `rowExceedsBatchLimit`. You may
change these limits when resuming. An error discards the pending batch while
preserving previously committed batches and their checkpoint.

NDJSON, SQLite and file I/O failures surface as `NdjsonImportException`. The error
provides a SQLite result code or OS error code when available. A rejected row
also provides its source line and byte offset. The exception omits SQL, bound
values, file paths, and the original database or OS error message.

Run the recovery and concurrency tests with `fvm dart test`.

Use `error.code` to handle failures without parsing error messages. NDJSON
validation errors use `NdjsonImportException`.

```dart
try {
  // await importNdjson(...);
} on NdjsonImportException catch (error) {
  print('${error.code.name}: line ${error.line}, byte ${error.byteOffset}');
}
```

| Code | Meaning |
| --- | --- |
| `invalidJson` | Invalid UTF-8, JSON, blank line, or embedded raw CR |
| `expectedObject` | JSON root is not an object |
| `missingMappedKey` | A configured JSON key is absent |
| `unsupportedValue` | A mapped value is an object or array |
| `lineTooLong` | Bytes before LF exceed `maxLineBytes` |
| `missingLineEnding` | The final nonempty line lacks LF |
| `rowExceedsBatchLimit` | One record including LF exceeds `maxBatchBytes` |
| `sqlite` / `io` | Database or filesystem failure |

Positions refer to the start of the rejected source line. Not every database
or I/O failure can be associated with a row. Argument errors and source,
mapping, or concurrent checkpoint conflicts remain `ArgumentError` and
`StateError`. Keep the source immutable throughout hashing and import.

Run `fvm dart run tool/benchmark.dart` for a local synthetic benchmark. It
includes source hashing and database writes, prints process peak RSS, and
removes its temporary source and databases even on failure. Results depend
on storage, SQLite settings, data shape, and runtime warm-up.
