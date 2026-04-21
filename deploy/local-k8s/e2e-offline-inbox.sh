#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_e2e_common.sh"

wait_greenmqtt_ready

tenant_id="${GREENMQTT_OFFLINE_TENANT_ID:-offline-${SUFFIX}}"
topic="${GREENMQTT_OFFLINE_TOPIC:-offline/${SUFFIX}/state}"
payload="${GREENMQTT_OFFLINE_PAYLOAD:-offline-message}"

run_curl_pod "e2e-offline" "
base='${HTTP_BASE}'
tenant='${tenant_id}'
topic='${topic}'
payload='${payload}'
payload_json=\$(printf '%s' \"\${payload}\" | od -An -t u1 | tr -s ' ' ',' | sed 's/^,//;s/,$//')

sub=\$(curl -fsS -H 'content-type: application/json' -d '{\"identity\":{\"tenant_id\":\"'\"\${tenant}\"'\",\"user_id\":\"offline-sub\",\"client_id\":\"offline-sub\"},\"node_id\":1,\"kind\":\"Persistent\",\"clean_start\":true,\"session_expiry_interval_secs\":60}' \"\${base}/v1/connect\")
sub_sid=\$(printf '%s' \"\${sub}\" | sed -n 's/.*\"session_id\":\"\\([^\"]*\\)\".*/\\1/p')
test -n \"\${sub_sid}\"

curl -fsS -H 'content-type: application/json' -d '{\"session_id\":\"'\"\${sub_sid}\"'\",\"topic_filter\":\"'\"\${topic}\"'\",\"qos\":1,\"subscription_identifier\":null,\"no_local\":false,\"retain_as_published\":false,\"retain_handling\":0,\"shared_group\":null}' \"\${base}/v1/subscribe\" >/dev/null
curl -fsS -H 'content-type: application/json' -d '{\"session_id\":\"'\"\${sub_sid}\"'\"}' \"\${base}/v1/disconnect\" >/dev/null

pub=\$(curl -fsS -H 'content-type: application/json' -d '{\"identity\":{\"tenant_id\":\"'\"\${tenant}\"'\",\"user_id\":\"offline-pub\",\"client_id\":\"offline-pub\"},\"node_id\":2,\"kind\":\"Transient\",\"clean_start\":true,\"session_expiry_interval_secs\":null}' \"\${base}/v1/connect\")
pub_sid=\$(printf '%s' \"\${pub}\" | sed -n 's/.*\"session_id\":\"\\([^\"]*\\)\".*/\\1/p')
test -n \"\${pub_sid}\"

curl -fsS -H 'content-type: application/json' -d '{\"session_id\":\"'\"\${pub_sid}\"'\",\"publish\":{\"topic\":\"'\"\${topic}\"'\",\"payload\":['\"\${payload_json}\"'],\"qos\":1,\"retain\":false,\"properties\":{\"payload_format_indicator\":null,\"content_type\":null,\"message_expiry_interval_secs\":null,\"stored_at_ms\":null,\"response_topic\":null,\"correlation_data\":null,\"subscription_identifiers\":[],\"user_properties\":[]}}}' \"\${base}/v1/publish\" >/dev/null

offline=\$(curl -fsS \"\${base}/v1/sessions/\${sub_sid}/offline\")
printf '%s\n' \"\${offline}\" | grep -q \"\${topic}\"

resume=\$(curl -fsS -H 'content-type: application/json' -d '{\"identity\":{\"tenant_id\":\"'\"\${tenant}\"'\",\"user_id\":\"offline-sub\",\"client_id\":\"offline-sub\"},\"node_id\":1,\"kind\":\"Persistent\",\"clean_start\":false,\"session_expiry_interval_secs\":60}' \"\${base}/v1/connect\")
printf '%s\n' \"\${resume}\" | grep -q '\"session_present\":true'
printf '%s\n' \"\${resume}\" | grep -q \"\${topic}\"
echo 'offline inbox replay ok'
"
