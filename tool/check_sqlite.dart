import 'package:sqlite3/sqlite3.dart';

void main() {
  final database = sqlite3.openInMemory();
  try {
    final version = database
        .select('SELECT sqlite_version() AS version')
        .single;
    database.execute('CREATE TABLE probe (value TEXT NOT NULL)');
    database.execute('INSERT INTO probe (value) VALUES (?)', ['ok']);
    final row = database.select('SELECT value FROM probe').single;
    if (row['value'] != 'ok') {
      throw StateError('SQLite did not return the inserted value.');
    }
    print('SQLite ${version['version']}: insert and read passed.');
  } finally {
    database.close();
  }
}
