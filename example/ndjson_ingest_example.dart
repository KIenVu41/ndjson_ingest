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
      onProgress: (progress) {
        print('Committed ${progress.rows} rows at byte ${progress.offset}.');
      },
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
