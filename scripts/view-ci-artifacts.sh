#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${1:-}"
if [[ -z "$ARTIFACT_DIR" ]]; then
  echo "usage: $0 <artifact-directory>" >&2
  exit 2
fi

if [[ ! -d "$ARTIFACT_DIR" ]]; then
  echo "artifact directory not found: $ARTIFACT_DIR" >&2
  exit 2
fi

echo "Artifacts:"
find "$ARTIFACT_DIR" -maxdepth 1 -type f | sort

echo
echo "Summary:"
ruby -rjson -e '
  puts "file\tprofile\tstatus"
  Dir[File.join(ARGV[0], "*.json")].sort.each do |path|
    begin
      summary = JSON.parse(File.read(path))
      puts "#{File.basename(path)}\t#{summary["profile"]}\t#{summary["status"]}"
    rescue JSON::ParserError
      puts "#{File.basename(path)}\tinvalid-json\t1"
    end
  end
' "$ARTIFACT_DIR"
