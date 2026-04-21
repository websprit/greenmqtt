#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_e2e_common.sh"

MULTIDIST_POD="${GREENMQTT_MULTIDIST_POD:-greenmqtt-multidist}"
MULTIDIST_HTTP_PORT="${GREENMQTT_MULTIDIST_HTTP_LOCAL_PORT:-28080}"
MULTIDIST_IMAGE="${GREENMQTT_MULTIDIST_IMAGE:-$(kubectl -n "${NAMESPACE}" get pod greenmqtt-0 -o jsonpath='{.spec.containers[0].image}')}"
MULTIDIST_BASE="http://127.0.0.1:${MULTIDIST_HTTP_PORT}"
MULTIDIST_TENANT="${GREENMQTT_MULTIDIST_TENANT:-multidist}"

cleanup() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
    wait "${PF_PID}" 2>/dev/null || true
  fi
  kubectl -n "${NAMESPACE}" delete pod "${MULTIDIST_POD}" --force --grace-period=0 >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_greenmqtt_ready

kubectl -n "${NAMESPACE}" delete pod "${MULTIDIST_POD}" --force --grace-period=0 >/dev/null 2>&1 || true

cat <<EOF >/tmp/${MULTIDIST_POD}.yaml
apiVersion: v1
kind: Pod
metadata:
  name: ${MULTIDIST_POD}
  namespace: ${NAMESPACE}
  labels:
    app: ${MULTIDIST_POD}
spec:
  restartPolicy: Never
  containers:
    - name: greenmqtt
      image: ${MULTIDIST_IMAGE}
      command: ["/usr/local/bin/greenmqtt", "serve"]
      env:
        - name: GREENMQTT_NODE_ID
          value: "1"
        - name: GREENMQTT_STORAGE_BACKEND
          value: "rocksdb"
        - name: GREENMQTT_DATA_DIR
          value: "/tmp/greenmqtt-data"
        - name: GREENMQTT_HTTP_BIND
          value: "0.0.0.0:8080"
        - name: GREENMQTT_MQTT_BIND
          value: "0.0.0.0:1883"
        - name: GREENMQTT_RPC_BIND
          value: "0.0.0.0:50051"
        - name: GREENMQTT_RESPONSE_INFORMATION
          value: "greenmqtt-multidist"
      ports:
        - containerPort: 8080
        - containerPort: 1883
        - containerPort: 50051
EOF

kubectl apply -f /tmp/${MULTIDIST_POD}.yaml >/dev/null
kubectl -n "${NAMESPACE}" wait --for=condition=ready pod/"${MULTIDIST_POD}" --timeout="${E2E_TIMEOUT}" >/dev/null

kubectl -n "${NAMESPACE}" port-forward pod/"${MULTIDIST_POD}" "${MULTIDIST_HTTP_PORT}:8080" >/tmp/${MULTIDIST_POD}-port-forward.log 2>&1 &
PF_PID=$!
sleep 2

curl -fsS "${MULTIDIST_BASE}/healthz" >/dev/null

export MULTIDIST_BASE
export MULTIDIST_TENANT

python3 - <<'PY'
import json
import os
import urllib.request

base = os.environ["MULTIDIST_BASE"]
tenant = os.environ["MULTIDIST_TENANT"]

def post(path, body):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(req) as response:
        raw = response.read().decode()
        return json.loads(raw) if raw else None

def get(path):
    with urllib.request.urlopen(base + path) as response:
        raw = response.read().decode()
        return json.loads(raw) if raw else None

common = {
    "epoch": 1,
    "config_version": 1,
    "leader_node_id": 1,
    "replicas": [
        {"node_id": 1, "role": "Voter", "sync_state": "Replicating"},
    ],
    "commit_index": 0,
    "applied_index": 0,
    "lifecycle": "Bootstrapping",
}

for descriptor in [
    {
        "id": "dist-range-a",
        "shard": {"kind": "Dist", "tenant_id": tenant, "scope": "*"},
        "boundary": {"start_key": None, "end_key": [109]},
        **common,
    },
    {
        "id": "dist-range-b",
        "shard": {"kind": "Dist", "tenant_id": tenant, "scope": "*"},
        "boundary": {"start_key": [109], "end_key": None},
        **common,
    },
    {
        "id": "sessiondict-range",
        "shard": {"kind": "SessionDict", "tenant_id": tenant, "scope": "*"},
        "boundary": {"start_key": None, "end_key": None},
        **common,
    },
    {
        "id": "inbox-range",
        "shard": {"kind": "Inbox", "tenant_id": tenant, "scope": "*"},
        "boundary": {"start_key": None, "end_key": None},
        **common,
    },
    {
        "id": "inflight-range",
        "shard": {"kind": "Inflight", "tenant_id": tenant, "scope": "*"},
        "boundary": {"start_key": None, "end_key": None},
        **common,
    },
    {
        "id": "retain-range",
        "shard": {"kind": "Retain", "tenant_id": tenant, "scope": "*"},
        "boundary": {"start_key": None, "end_key": None},
        **common,
    },
]:
    reply = post("/v1/ranges/bootstrap", {"descriptor": descriptor})
    assert reply["status"] == "bootstrapped", reply

sub = post(
    "/v1/connect",
    {
        "identity": {
            "tenant_id": tenant,
            "user_id": "sub",
            "client_id": "sub",
        },
        "node_id": 1,
        "kind": "Persistent",
        "clean_start": True,
        "session_expiry_interval_secs": 60,
    },
)
sub_sid = sub["session"]["session_id"]

for topic in ["alerts/a", "zeta/a"]:
    post(
        "/v1/subscribe",
        {
            "session_id": sub_sid,
            "topic_filter": topic,
            "qos": 1,
            "subscription_identifier": None,
            "no_local": False,
            "retain_as_published": False,
            "retain_handling": 0,
            "shared_group": None,
        },
    )

pub = post(
    "/v1/connect",
    {
        "identity": {
            "tenant_id": tenant,
            "user_id": "pub",
            "client_id": "pub",
        },
        "node_id": 1,
        "kind": "Transient",
        "clean_start": True,
        "session_expiry_interval_secs": None,
    },
)
pub_sid = pub["session"]["session_id"]

publish_1 = post(
    "/v1/publish",
    {
        "session_id": pub_sid,
        "publish": {
            "topic": "alerts/a",
            "payload": [97, 108, 101, 114, 116, 115],
            "qos": 1,
            "retain": False,
            "properties": {
                "payload_format_indicator": None,
                "content_type": None,
                "message_expiry_interval_secs": None,
                "stored_at_ms": None,
                "response_topic": None,
                "correlation_data": None,
                "subscription_identifiers": [],
                "user_properties": [],
            },
        },
    },
)
assert publish_1["matched_routes"] == 1, publish_1
assert publish_1["online_deliveries"] == 1, publish_1

publish_2 = post(
    "/v1/publish",
    {
        "session_id": pub_sid,
        "publish": {
            "topic": "zeta/a",
            "payload": [122, 101, 116, 97],
            "qos": 1,
            "retain": False,
            "properties": {
                "payload_format_indicator": None,
                "content_type": None,
                "message_expiry_interval_secs": None,
                "stored_at_ms": None,
                "response_topic": None,
                "correlation_data": None,
                "subscription_identifiers": [],
                "user_properties": [],
            },
        },
    },
)
assert publish_2["matched_routes"] == 1, publish_2
assert publish_2["online_deliveries"] == 1, publish_2

post(
    "/v1/publish",
    {
        "session_id": pub_sid,
        "publish": {
            "topic": "zeta/retain",
            "payload": [114, 101, 116, 97, 105, 110],
            "qos": 1,
            "retain": True,
            "properties": {
                "payload_format_indicator": None,
                "content_type": None,
                "message_expiry_interval_secs": None,
                "stored_at_ms": None,
                "response_topic": None,
                "correlation_data": None,
                "subscription_identifiers": [],
                "user_properties": [],
            },
        },
    },
)
retained = get(f"/v1/retain?tenant_id={tenant}&topic_filter=zeta/retain")
assert len(retained) == 1, retained

post("/v1/disconnect", {"session_id": sub_sid})

publish_offline = post(
    "/v1/publish",
    {
        "session_id": pub_sid,
        "publish": {
            "topic": "alerts/a",
            "payload": [111, 102, 102, 108, 105, 110, 101],
            "qos": 1,
            "retain": False,
            "properties": {
                "payload_format_indicator": None,
                "content_type": None,
                "message_expiry_interval_secs": None,
                "stored_at_ms": None,
                "response_topic": None,
                "correlation_data": None,
                "subscription_identifiers": [],
                "user_properties": [],
            },
        },
    },
)
assert publish_offline["offline_enqueues"] >= 1, publish_offline

offline = get(f"/v1/sessions/{sub_sid}/offline")
assert len(offline) >= 1, offline

resume = post(
    "/v1/connect",
    {
        "identity": {
            "tenant_id": tenant,
            "user_id": "sub",
            "client_id": "sub",
        },
        "node_id": 1,
        "kind": "Persistent",
        "clean_start": False,
        "session_expiry_interval_secs": 60,
    },
)
assert resume["session_present"] is True, resume
assert len(resume["offline_messages"]) >= 1, resume

print(
    json.dumps(
        {
            "tenant": tenant,
            "sub_session_id": sub_sid,
            "publish1": publish_1,
            "publish2": publish_2,
            "retained_count": len(retained),
            "offline_count": len(offline),
            "resume_present": resume["session_present"],
            "resume_offline_count": len(resume["offline_messages"]),
        }
    )
)
PY","workdir":"/Users/ugreen/Documents/iot_arch/greenmqtt","yield_time_ms":1000,"max_output_tokens":12000,"env":{"MULTIDIST_BASE":"http://127.0.0.1:28080","MULTIDIST_TENANT":"multidist"}}]}: function exec_command({"cmd":"python3 - <<'PY'\nimport json\nimport os\nimport urllib.request\n\nbase = os.environ[\"MULTIDIST_BASE\"]\ntenant = os.environ[\"MULTIDIST_TENANT\"]\n\ndef post(path, body):\n    req = urllib.request.Request(\n        base + path,\n        data=json.dumps(body).encode(),\n        headers={\"content-type\": \"application/json\"},\n    )\n    with urllib.request.urlopen(req) as response:\n        raw = response.read().decode()\n        return json.loads(raw) if raw else None\n\ndef get(path):\n    with urllib.request.urlopen(base + path) as response:\n        raw = response.read().decode()\n        return json.loads(raw) if raw else None\n\ncommon = {\n    \"epoch\": 1,\n    \"config_version\": 1,\n    \"leader_node_id\": 1,\n    \"replicas\": [\n        {\"node_id\": 1, \"role\": \"Voter\", \"sync_state\": \"Replicating\"},\n    ],\n    \"commit_index\": 0,\n    \"applied_index\": 0,\n    \"lifecycle\": \"Bootstrapping\",\n}\n\nfor descriptor in [\n    {\n        \"id\": \"dist-range-a\",\n        \"shard\": {\"kind\": \"Dist\", \"tenant_id\": tenant, \"scope\": \"*\"},\n        \"boundary\": {\"start_key\": None, \"end_key\": [109]},\n        **common,\n    },\n    {\n        \"id\": \"dist-range-b\",\n        \"shard\": {\"kind\": \"Dist\", \"tenant_id\": tenant, \"scope\": \"*\"},\n        \"boundary\": {\"start_key\": [109], \"end_key\": None},\n        **common,\n    },\n    {\n        \"id\": \"sessiondict-range\",\n        \"shard\": {\"kind\": \"SessionDict\", \"tenant_id\": tenant, \"scope\": \"*\"},\n        \"boundary\": {\"start_key\": None, \"end_key\": None},\n        **common,\n    },\n    {\n        \"id\": \"inbox-range\",\n        \"shard\": {\"kind\": \"Inbox\", \"tenant_id\": tenant, \"scope\": \"*\"},\n        \"boundary\": {\"start_key\": None, \"end_key\": None},\n        **common,\n    },\n    {\n        \"id\": \"inflight-range\",\n        \"shard\": {\"kind\": \"Inflight\", \"tenant_id\": tenant, \"scope\": \"*\"},\n        \"boundary\": {\"start_key\": None, \"end_key\": None},\n        **common,\n    },\n    {\n        \"id\": \"retain-range\",\n        \"shard\": {\"kind\": \"Retain\", \"tenant_id\": tenant, \"scope\": \"*\"},\n        \"boundary\": {\"start_key\": None, \"end_key\": None},\n        **common,\n    },\n]:\n    reply = post(\"/v1/ranges/bootstrap\", {\"descriptor\": descriptor})\n    assert reply[\"status\"] == \"bootstrapped\", reply\n\nsub = post(\n    \"/v1/connect\",\n    {\n        \"identity\": {\n            \"tenant_id\": tenant,\n            \"user_id\": \"sub\",\n            \"client_id\": \"sub\",\n        },\n        \"node_id\": 1,\n        \"kind\": \"Persistent\",\n        \"clean_start\": True,\n        \"session_expiry_interval_secs\": 60,\n    },\n)\nsub_sid = sub[\"session\"][\"session_id\"]\n\nfor topic in [\"alerts/a\", \"zeta/a\"]:\n    post(\n        \"/v1/subscribe\",\n        {\n            \"session_id\": sub_sid,\n            \"topic_filter\": topic,\n            \"qos\": 1,\n            \"subscription_identifier\": None,\n            \"no_local\": False,\n            \"retain_as_published\": False,\n            \"retain_handling\": 0,\n            \"shared_group\": None,\n        },\n    )\n\npub = post(\n    \"/v1/connect\",\n    {\n        \"identity\": {\n            \"tenant_id\": tenant,\n            \"user_id\": \"pub\",\n            \"client_id\": \"pub\",\n        },\n        \"node_id\": 1,\n        \"kind\": \"Transient\",\n        \"clean_start\": True,\n        \"session_expiry_interval_secs\": None,\n    },\n)\npub_sid = pub[\"session\"][\"session_id\"]\n\npublish_1 = post(\n    \"/v1/publish\",\n    {\n        \"session_id\": pub_sid,\n        \"publish\": {\n            \"topic\": \"alerts/a\",\n            \"payload\": [97, 108, 101, 114, 116, 115],\n            \"qos\": 1,\n            \"retain\": False,\n            \"properties\": {\n                \"payload_format_indicator\": None,\n                \"content_type\": None,\n                \"message_expiry_interval_secs\": None,\n                \"stored_at_ms\": None,\n                \"response_topic\": None,\n                \"correlation_data\": None,\n                \"subscription_identifiers\": [],\n                \"user_properties\": [],\n            },\n        },\n    },\n)\nassert publish_1[\"matched_routes\"] == 1, publish_1\nassert publish_1[\"online_deliveries\"] == 1, publish_1\n\npublish_2 = post(\n    \"/v1/publish\",\n    {\n        \"session_id\": pub_sid,\n        \"publish\": {\n            \"topic\": \"zeta/a\",\n            \"payload\": [122, 101, 116, 97],\n            \"qos\": 1,\n            \"retain\": False,\n            \"properties\": {\n                \"payload_format_indicator\": None,\n                \"content_type\": None,\n                \"message_expiry_interval_secs\": None,\n                \"stored_at_ms\": None,\n                \"response_topic\": None,\n                \"correlation_data\": None,\n                \"subscription_identifiers\": [],\n                \"user_properties\": [],\n            },\n        },\n    },\n)\nassert publish_2[\"matched_routes\"] == 1, publish_2\nassert publish_2[\"online_deliveries\"] == 1, publish_2\n\npost(\n    \"/v1/publish\",\n    {\n        \"session_id\": pub_sid,\n        \"publish\": {\n            \"topic\": \"zeta/retain\",\n            \"payload\": [114, 101, 116, 97, 105, 110],\n            \"qos\": 1,\n            \"retain\": True,\n            \"properties\": {\n                \"payload_format_indicator\": None,\n                \"content_type\": None,\n                \"message_expiry_interval_secs\": None,\n                \"stored_at_ms\": None,\n                \"response_topic\": None,\n                \"correlation_data\": None,\n                \"subscription_identifiers\": [],\n                \"user_properties\": [],\n            },\n        },\n    },\n)\nretained = get(f\"/v1/retain?tenant_id={tenant}&topic_filter=zeta/retain\")\nassert len(retained) == 1, retained\n\npost(\"/v1/disconnect\", {\"session_id\": sub_sid})\n\npublish_offline = post(\n    \"/v1/publish\",\n    {\n        \"session_id\": pub_sid,\n        \"publish\": {\n            \"topic\": \"alerts/a\",\n            \"payload\": [111, 102, 102, 108, 105, 110, 101],\n            \"qos\": 1,\n            \"retain\": False,\n            \"properties\": {\n                \"payload_format_indicator\": None,\n                \"content_type\": None,\n                \"message_expiry_interval_secs\": None,\n                \"stored_at_ms\": None,\n                \"response_topic\": None,\n                \"correlation_data\": None,\n                \"subscription_identifiers\": [],\n                \"user_properties\": [],\n            },\n        },\n    },\n)\nassert publish_offline[\"offline_enqueues\"] >= 1, publish_offline\n\noffline = get(f\"/v1/sessions/{sub_sid}/offline\")\nassert len(offline) >= 1, offline\n\nresume = post(\n    \"/v1/connect\",\n    {\n        \"identity\": {\n            \"tenant_id\": tenant,\n            \"user_id\": \"sub\",\n            \"client_id\": \"sub\",\n        },\n        \"node_id\": 1,\n        \"kind\": \"Persistent\",\n        \"clean_start\": False,\n        \"session_expiry_interval_secs\": 60,\n    },\n)\nassert resume[\"session_present\"] is True, resume\nassert len(resume[\"offline_messages\"]) >= 1, resume\n\nprint(\n    json.dumps(\n        {\n            \"tenant\": tenant,\n            \"sub_session_id\": sub_sid,\n            \"publish1\": publish_1,\n            \"publish2\": publish_2,\n            \"retained_count\": len(retained),\n            \"offline_count\": len(offline),\n            \"resume_present\": resume[\"session_present\"],\n            \"resume_offline_count\": len(resume[\"offline_messages\"]),\n        }\n    )\n)\nPY","workdir":"/Users/ugreen/Documents/iot_arch/greenmqtt","yield_time_ms":1000,"max_output_tokens":12000,"env":{"MULTIDIST_BASE":"http://127.0.0.1:28080","MULTIDIST_TENANT":"multidist"}}) got an unexpected keyword argument 'env'
