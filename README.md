# ndjson_ingest

Import local NDJSON files into SQLite in a worker isolate, with bounded
batches and atomic checkpoints for resuming after the process stops.
This package is an early prerelease for native Dart and Flutter applications.

## Development

Use Dart 3.13.4, available via the pinned Flutter 3.47.5 SDK in FVM.
The minimum is Dart 3.10.0. The native SQLite build and import tests have been
verified on macOS arm64 and Ubuntu. The importer uses native Dart I/O.

```sh
fvm dart pub get
fvm dart analyze
fvm dart run tool/check_sqlite.dart
```

The SQLite check opens an in-memory database using the native library supplied
by the SQLite build hook. It prints the engine version and verifies a bound
insert and read.

## Import prototype

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

Every line needs an LF or CRLF ending. The prototype rejects blank lines and
limits a line to 256 KiB and a batch to 500 rows or 4 MiB of source bytes,
whichever comes first. A single worker isolate commits rows and the checkpoint
in one transaction. Duplicate IDs or an interrupted transaction roll back the
current batch. Earlier committed batches remain available.

## Import into your own table

Create the destination table before calling the API. The following mapping
imports `{"sku":"A-01","title":"Cà phê","active":true}` into an existing
`inventory(item_id, label, enabled)` table.

```dart
import 'package:ndjson_ingest/ndjson_ingest.dart';

Future<void> main() async {
  final progress = await importNdjson(
    sourcePath: 'inventory.ndjson',
    databasePath: 'app.sqlite',
    jobId: 'inventory-v1',
    table: 'inventory',
    columns: const {'sku': 'item_id', 'title': 'label', 'active': 'enabled'},
    batchRows: 500,
    maxBatchBytes: 4 * 1024 * 1024,
    maxLineBytes: 256 * 1024,
  );
  print(progress.rows);
}
```

Each mapped key must be present. Supported values are strings, numbers,
booleans and null. Booleans bind as SQLite integers, and the destination table
enforces its constraints. Committed batches are visible during the import.

Resume requires the same source content, target table, mapping and
`mappingVersion`. Change `mappingVersion` when a schema or interpretation
change alters what the same mapping means. An incompatible job requires a
new job ID. Existing rows remain in the destination, so the application
must decide how to handle them before importing again.

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

The [runnable example](example/ndjson_ingest_example.dart) creates a temporary
inventory database, pauses after one batch, resumes the same job, and cleans
up its files. Run it with `fvm dart run example/ndjson_ingest_example.dart`.

Use `error.code` to handle failures without parsing error messages. NDJSON
validation errors now use `NdjsonImportException` rather than `FormatException`.

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
