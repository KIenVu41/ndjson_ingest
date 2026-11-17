# ndjson_ingest

A Dart VM project for importing local NDJSON files into SQLite. The first
milestone is a CLI prototype that can resume from the last committed batch
after the process stops.

## Development

Use Dart 3.13.4, available via the pinned Flutter 3.47.5 SDK in FVM.
The minimum is Dart 3.10.0. The native SQLite build and import tests have been
verified on macOS arm64.

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
SHA-256 hash of the source before it resumes.

Every line needs an LF or CRLF ending. The prototype rejects blank lines and
limits a line to 256 KiB and a batch to 500 rows or 4 MiB of source bytes,
whichever comes first. A single worker isolate commits rows and the checkpoint
in one transaction. Duplicate IDs cause the current batch to roll back.

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

Run the recovery and concurrency tests with `fvm dart test`.

The [runnable example](example/ndjson_ingest_example.dart) creates a temporary
inventory database, pauses after one batch, resumes the same job, and cleans
up its files. Run it with `fvm dart run example/ndjson_ingest_example.dart`.
