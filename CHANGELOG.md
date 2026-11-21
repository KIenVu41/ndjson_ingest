## 0.1.0

- First stable release for Dart VM imports on Linux and macOS.
- Retain the import API and checkpoint format from 0.1.0-dev.2.
- Document installation, SDK requirements and verified platform scope.
- Include a complete table-creation, pause/resume and cleanup example.
- Add macOS CI alongside the existing Ubuntu SDK checks.
- Flutter applications, Windows and web are outside this release's support scope.

## 0.1.0-dev.2

- Configure transaction row count, source byte limit, and line byte limit.
- Allow changing batch limits while resuming an existing checkpoint.
- Expose sanitized NDJSON errors with stable codes, source line and byte offset.
- NDJSON validation now throws NdjsonImportException instead of FormatException.
- Reject a single record larger than the configured batch byte limit.
- Add boundary, rollback, resume and error redaction regression tests.
- Document byte accounting and the full-source hash cost on resume.
- Add a synthetic benchmark that cleans up temporary files.

## 0.1.0-dev.1

- Import local NDJSON into an existing SQLite table using a column mapping.
- Commit each batch and its resume checkpoint together in a worker isolate.
- Verify source content and mapping before resuming an import.
- Redact SQL, row values, and file paths from public SQLite and I/O errors.
- Include a CLI prototype and a runnable resume example.
