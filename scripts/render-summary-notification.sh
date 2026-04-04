#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <summary.json>" >&2
  exit 2
fi

summary_file="$1"

ruby -rjson -e '
  summary = JSON.parse(File.read(ARGV[0]))

  entries =
    if summary["components"].is_a?(Array)
      summary["components"]
    elsif summary["results"].is_a?(Array)
      summary["results"]
    else
      []
    end

  regressions = []
  advisories = []

  entries.each do |entry|
    status = entry["status"].to_i
    next if status == 0

    required =
      if entry.key?("required")
        entry["required"]
      else
        true
      end

    payload = {
      "name" => entry["name"] || entry["profile"] || "unnamed",
      "status" => status,
    }

    if required
      regressions << payload
    else
      advisories << payload
    end
  end

  if summary["status"].to_i != 0 && regressions.empty? && advisories.empty?
    regressions << {
      "name" => summary["profile"] || "summary",
      "status" => summary["status"],
    }
  end

  level =
    if regressions.any?
      "error"
    elsif advisories.any?
      "warning"
    else
      "info"
    end

  message =
    if regressions.any?
      failed = regressions.map { |item| "#{item["name"]}(status=#{item["status"]})" }.join(", ")
      "#{summary["profile"]} has blocking regressions: #{failed}"
    elsif advisories.any?
      warning = advisories.map { |item| "#{item["name"]}(status=#{item["status"]})" }.join(", ")
      "#{summary["profile"]} has advisory fluctuations: #{warning}"
    else
      "#{summary["profile"]} is green"
    end

  puts JSON.generate({
    profile: "summary-notification",
    source_profile: summary["profile"],
    level: level,
    status: summary["status"],
    message: message,
    regressions: regressions,
    advisories: advisories,
  })
' "$summary_file"
