#!/usr/bin/env bash
set -euo pipefail

SUMMARY_FILE="${GREENMQTT_PROFILE_SUMMARY_FILE:-}"
METRICS_FILE="${GREENMQTT_METRICS_FILE:-}"
HTTP_BIND="${GREENMQTT_HTTP_BIND:-127.0.0.1:8080}"

fetch_metrics() {
  if [[ -n "$METRICS_FILE" ]]; then
    cat "$METRICS_FILE"
    return 0
  fi

  ruby -rsocket -e '
    host, port = ARGV[0].split(":", 2)
    socket = TCPSocket.new(host, Integer(port))
    socket.write("GET /metrics HTTP/1.1\r\nHost: #{ARGV[0]}\r\nConnection: close\r\n\r\n")
    socket.close_write
    response = socket.read
    body = response.split("\r\n\r\n", 2)[1]
    abort("invalid http response") if body.nil?
    print body
  ' "$HTTP_BIND"
}

emit_summary() {
  local summary="$1"
  if [[ -n "$SUMMARY_FILE" ]]; then
    printf '%s\n' "$summary" > "$SUMMARY_FILE"
  fi
  printf '%s\n' "$summary"
}

metrics_text="$(fetch_metrics)"

summary="$(ruby -rjson -e '
  text = STDIN.read
  interesting = {
    "mqtt_shard_move_total" => 0.0,
    "mqtt_shard_failover_total" => 0.0,
    "mqtt_shard_anti_entropy_total" => 0.0,
    "mqtt_shard_fencing_reject_total" => 0.0,
  }
  labels = Hash.new { |hash, key| hash[key] = [] }

  text.each_line do |line|
    line = line.strip
    next if line.empty? || line.start_with?("#")
    if line =~ /\A([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]+\})?\s+([0-9eE+\-.]+)\z/
      metric = Regexp.last_match(1)
      raw_labels = Regexp.last_match(2)
      value = Regexp.last_match(3).to_f
      next unless interesting.key?(metric)
      interesting[metric] += value
      labels[metric] << { labels: raw_labels, value: value } if raw_labels
    end
  end

  puts JSON.generate({
    profile: "shard-metrics-snapshot",
    status: 0,
    metrics: interesting,
    labeled_samples: labels,
  })
' <<< "$metrics_text")"

emit_summary "$summary"
