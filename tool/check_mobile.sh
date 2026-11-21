#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 2 || ( "$1" != android && "$1" != ios ) ]]; then
  echo "Usage: FLUTTER_BIN=/path/to/flutter bash tool/check_mobile.sh android|ios device-id" >&2
  exit 2
fi

platform="$1"
device="$2"
package_root="$(cd "$(dirname "$0")/.." && pwd)"
flutter_bin="${FLUTTER_BIN:-flutter}"
workspace="$(mktemp -d "${TMPDIR:-/tmp}/ndjson-mobile.XXXXXX")"
app_started=false
cleanup() {
  if [[ "$app_started" == true ]]; then
    (cd "$workspace/app" && "$flutter_bin" install --uninstall-only -d "$device") || true
  fi
  rm -rf "$workspace"
}
trap cleanup EXIT

"$flutter_bin" create --no-pub --platforms="$platform" --project-name ndjson_mobile_check "$workspace/app"
cat > "$workspace/app/pubspec.yaml" <<YAML
name: ndjson_mobile_check
version: 1.0.0+1
publish_to: none
environment:
  sdk: ^3.10.0
dependencies:
  flutter:
    sdk: flutter
  ndjson_ingest:
    path: '$package_root'
  sqlite3: ^3.9.1
dev_dependencies:
  flutter_test:
    sdk: flutter
  integration_test:
    sdk: flutter
YAML
mkdir -p "$workspace/app/integration_test"
cp "$package_root/tool/mobile/import_test.dart.txt" "$workspace/app/integration_test/import_test.dart"
cd "$workspace/app"
"$flutter_bin" pub get
app_started=true
"$flutter_bin" test integration_test/import_test.dart -d "$device" --reporter expanded
if [[ "$platform" == android ]]; then
  "$flutter_bin" build apk --release --target=integration_test/import_test.dart
else
  "$flutter_bin" build ios --release --no-codesign --target=integration_test/import_test.dart
fi
