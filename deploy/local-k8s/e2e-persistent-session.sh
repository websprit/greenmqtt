#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_e2e_common.sh"

wait_greenmqtt_ready

tenant_id="${GREENMQTT_PERSISTENT_TENANT_ID:-persistent-${SUFFIX}}"
topic_filter="${GREENMQTT_PERSISTENT_TOPIC_FILTER:-persist/${SUFFIX}/state}"
user_id="${GREENMQTT_PERSISTENT_USER_ID:-persist-user}"
client_id="${GREENMQTT_PERSISTENT_CLIENT_ID:-persist-client}"

run_curl_pod "e2e-persistent" "
base='${HTTP_BASE}'
tenant='${tenant_id}'
topic='${topic_filter}'
user_id='${user_id}'
client_id='${client_id}'

first=\$(curl -fsS -H 'content-type: application/json' -d '{\"identity\":{\"tenant_id\":\"'\"\${tenant}\"'\",\"user_id\":\"'\"\${user_id}\"'\",\"client_id\":\"'\"\${client_id}\"'\"},\"node_id\":1,\"kind\":\"Persistent\",\"clean_start\":true,\"session_expiry_interval_secs\":60}' \"\${base}/v1/connect\")
first_sid=\$(printf '%s' \"\${first}\" | sed -n 's/.*\"session_id\":\"\\([^\"]*\\)\".*/\\1/p')
test -n \"\${first_sid}\"
printf '%s\n' \"\${first}\" | grep -q '\"session_present\":false'

curl -fsS -H 'content-type: application/json' -d '{\"session_id\":\"'\"\${first_sid}\"'\",\"topic_filter\":\"'\"\${topic}\"'\",\"qos\":1,\"subscription_identifier\":null,\"no_local\":false,\"retain_as_published\":false,\"retain_handling\":0,\"shared_group\":null}' \"\${base}/v1/subscribe\" >/dev/null
curl -fsS -H 'content-type: application/json' -d '{\"session_id\":\"'\"\${first_sid}\"'\"}' \"\${base}/v1/disconnect\" >/dev/null

second=\$(curl -fsS -H 'content-type: application/json' -d '{\"identity\":{\"tenant_id\":\"'\"\${tenant}\"'\",\"user_id\":\"'\"\${user_id}\"'\",\"client_id\":\"'\"\${client_id}\"'\"},\"node_id\":1,\"kind\":\"Persistent\",\"clean_start\":false,\"session_expiry_interval_secs\":60}' \"\${base}/v1/connect\")
second_sid=\$(printf '%s' \"\${second}\" | sed -n 's/.*\"session_id\":\"\\([^\"]*\\)\".*/\\1/p')
test -n \"\${second_sid}\"
printf '%s\n' \"\${second}\" | grep -q '\"session_present\":true'

subs=\$(curl -fsS \"\${base}/v1/sessions/\${second_sid}/subscriptions\")
printf '%s\n' \"\${subs}\" | grep -q \"\${topic}\"
echo 'persistent session recovery ok'
"
