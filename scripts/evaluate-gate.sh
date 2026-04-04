#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <release|nightly|post-release> <summary.json>" >&2
  exit 2
fi

gate_name="$1"
summary_file="$2"

ruby -rjson -e '
  gate_name = ARGV[0]
  summary = JSON.parse(File.read(ARGV[1]))

  entries =
    if summary["components"].is_a?(Array)
      summary["components"]
    elsif summary["results"].is_a?(Array)
      summary["results"]
    else
      []
    end

  required_failures = []
  advisory_failures = []

  entries.each do |entry|
    status = entry["status"].to_i
    next if status == 0
    required = entry.key?("required") ? entry["required"] : true
    payload = {
      "name" => entry["name"] || entry["profile"] || "unnamed",
      "status" => status,
    }
    if required
      required_failures << payload
    else
      advisory_failures << payload
    end
  end

  if summary["status"].to_i != 0 && required_failures.empty? && entries.empty?
    required_failures << {
      "name" => summary["profile"] || gate_name,
      "status" => summary["status"],
    }
  end

  decision = required_failures.empty? ? "pass" : "block"

  puts JSON.generate({
    profile: "gate-evaluation",
    gate: gate_name,
    source_profile: summary["profile"],
    decision: decision,
    status: decision == "pass" ? 0 : 1,
    required_failures: required_failures,
    advisory_failures: advisory_failures,
  })
' "$gate_name" "$summary_file"
