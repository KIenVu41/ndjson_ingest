## 0.1.0-dev.1

- Import local NDJSON into an existing SQLite table using a column mapping.
- Commit each batch and its resume checkpoint together in a worker isolate.
- Verify source content and mapping before resuming an import.
- Redact SQL, row values, and file paths from public SQLite and I/O errors.
- Include a CLI prototype and a runnable resume example.
