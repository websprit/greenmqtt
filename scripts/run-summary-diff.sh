#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <old-summary.json> <new-summary.json>" >&2
  exit 2
fi

old_file="$1"
new_file="$2"

ruby -rjson -rtime -e '
  def index_entries(entries)
    return {} unless entries.is_a?(Array)
    entries.each_with_object({}) do |entry, acc|
      name = entry["name"] || entry["profile"]
      acc[name] = entry if name
    end
  end

  def compare_named_entries(old_entries, new_entries)
    old_index = index_entries(old_entries)
    new_index = index_entries(new_entries)
    names = (old_index.keys + new_index.keys).uniq.sort

    names.map do |name|
      old_entry = old_index[name]
      new_entry = new_index[name]
      next({
        name: name,
        change: "added",
        required: new_entry&.fetch("required", true),
        old_status: nil,
        new_status: new_entry && new_entry["status"],
      }) if old_entry.nil?
      next({
        name: name,
        change: "removed",
        required: old_entry.fetch("required", true),
        old_status: old_entry["status"],
        new_status: nil,
      }) if new_entry.nil?

      {
        name: name,
        change: old_entry["status"] == new_entry["status"] ? "unchanged" : "changed",
        required: new_entry.fetch("required", true),
        old_status: old_entry["status"],
        new_status: new_entry["status"],
      }
    end.compact
  end

  old_summary = JSON.parse(File.read(ARGV[0]))
  new_summary = JSON.parse(File.read(ARGV[1]))

  old_entries = old_summary["components"] || old_summary["results"] || []
  new_entries = new_summary["components"] || new_summary["results"] || []
  entries = compare_named_entries(old_entries, new_entries)

  regressions = []
  advisories = []

  if old_summary["status"].to_i < new_summary["status"].to_i
    regressions << {
      scope: "summary",
      reason: "overall status regressed",
      old_status: old_summary["status"],
      new_status: new_summary["status"],
    }
  end

  entries.each do |entry|
    old_status = entry[:old_status]
    new_status = entry[:new_status]
    next if old_status.nil? || new_status.nil?
    next unless old_status.to_i < new_status.to_i

    payload = {
      scope: entry[:name],
      reason: "status regressed",
      old_status: old_status,
      new_status: new_status,
    }

    if entry[:required]
      regressions << payload
    else
      advisories << payload
    end
  end

  if new_summary["components"].is_a?(Array)
    new_summary["components"].each do |component|
      next if component.fetch("required", true)
      next unless component["status"].to_i != 0
      advisories << {
        scope: component["name"],
        reason: "advisory component failed",
        old_status: nil,
        new_status: component["status"],
      }
    end
  end

  report = {
    profile: "summary-diff",
    compared_profile: new_summary["profile"],
    old_profile: old_summary["profile"],
    new_profile: new_summary["profile"],
    status: regressions.empty? ? 0 : 1,
    generated_at: Time.now.utc.iso8601,
    regressions: regressions,
    advisories: advisories.uniq,
    entries: entries,
  }

  puts JSON.generate(report)
' "$old_file" "$new_file"
