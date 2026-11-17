import 'dart:io';

import 'package:ndjson_ingest/ndjson_ingest.dart';

Future<void> main(List<String> args) async {
  final progress = await importNdjson(
    sourcePath: args[0],
    databasePath: args[1],
    jobId: 'catalog',
    table: 'inventory',
    columns: const {'sku': 'item_id', 'title': 'label', 'active': 'enabled'},
    maxBatches: 1,
  );
  stderr.writeln('NDJSON_COMMITTED ${progress.rows}');
  await stderr.flush();
  await stdin.take(1).drain<void>();
}
