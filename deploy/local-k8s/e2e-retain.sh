#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_e2e_common.sh"

wait_greenmqtt_ready

tenant_id="${GREENMQTT_RETAIN_TENANT_ID:-retain-${SUFFIX}}"
topic="${GREENMQTT_RETAIN_TOPIC:-retain/${SUFFIX}/state}"
payload="${GREENMQTT_RETAIN_PAYLOAD:-retained-mqtt}"

run_curl_pod "e2e-retain-http" "
base='${HTTP_BASE}'
tenant='${tenant_id}'
topic='${topic}'
payload='${payload}'
payload_json=\$(printf '%s' \"\${payload}\" | od -An -t u1 | tr -s ' ' ',' | sed 's/^,//;s/,$//')

pub=\$(curl -fsS -H 'content-type: application/json' -d '{\"identity\":{\"tenant_id\":\"'\"\${tenant}\"'\",\"user_id\":\"retain-pub\",\"client_id\":\"retain-pub\"},\"node_id\":1,\"kind\":\"Transient\",\"clean_start\":true,\"session_expiry_interval_secs\":null}' \"\${base}/v1/connect\")
pub_sid=\$(printf '%s' \"\${pub}\" | sed -n 's/.*\"session_id\":\"\\([^\"]*\\)\".*/\\1/p')
test -n \"\${pub_sid}\"

curl -fsS -H 'content-type: application/json' -d '{\"session_id\":\"'\"\${pub_sid}\"'\",\"publish\":{\"topic\":\"'\"\${topic}\"'\",\"payload\":['\"\${payload_json}\"'],\"qos\":1,\"retain\":true,\"properties\":{\"payload_format_indicator\":null,\"content_type\":null,\"message_expiry_interval_secs\":null,\"stored_at_ms\":null,\"response_topic\":null,\"correlation_data\":null,\"subscription_identifiers\":[],\"user_properties\":[]}}}' \"\${base}/v1/publish\" >/dev/null

retained=\$(curl -fsS \"\${base}/v1/retain?tenant_id=\${tenant}&topic_filter=\${topic}\")
printf '%s\n' \"\${retained}\" | grep -q \"\${topic}\"
echo 'retain http query ok'
"

run_mqtt_pod "e2e-retain-mqtt" "
topic='${topic}'
payload='${payload}'
host='${MQTT_HOST}'
port='${MQTT_PORT}'

mosquitto_sub -h \"\${host}\" -p \"\${port}\" -t \"\${topic}\" -C 1 -W 10 >/tmp/msg
grep -q \"\${payload}\" /tmp/msg
cat /tmp/msg
echo 'retain mqtt replay ok'
"
