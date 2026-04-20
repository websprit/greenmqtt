{{/*
Expand the name of the chart.
*/}}
{{- define "greenmqtt.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "greenmqtt.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Chart label.
*/}}
{{- define "greenmqtt.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "greenmqtt.labels" -}}
helm.sh/chart: {{ include "greenmqtt.chart" . }}
{{ include "greenmqtt.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "greenmqtt.selectorLabels" -}}
app.kubernetes.io/name: {{ include "greenmqtt.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Service account name.
*/}}
{{- define "greenmqtt.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "greenmqtt.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Headless service name.
*/}}
{{- define "greenmqtt.headlessServiceName" -}}
{{- printf "%s-headless" (include "greenmqtt.fullname" .) -}}
{{- end -}}

{{/*
Store service name.
*/}}
{{- define "greenmqtt.storeFullname" -}}
{{- printf "%s-state" (include "greenmqtt.fullname" .) -}}
{{- end -}}

{{/*
Store headless service name.
*/}}
{{- define "greenmqtt.storeHeadlessServiceName" -}}
{{- printf "%s-headless" (include "greenmqtt.storeFullname" .) -}}
{{- end -}}

{{/*
Rendered broker environment variables.
*/}}
{{- define "greenmqtt.env" -}}
- name: GREENMQTT_MQTT_BIND
  value: {{ .Values.greenmqtt.mqttBind | quote }}
- name: GREENMQTT_HTTP_BIND
  value: {{ .Values.greenmqtt.httpBind | quote }}
- name: GREENMQTT_RPC_BIND
  value: {{ .Values.greenmqtt.rpcBind | quote }}
- name: GREENMQTT_STORAGE_BACKEND
  value: {{ .Values.greenmqtt.storageBackend | quote }}
- name: GREENMQTT_DATA_DIR
  value: {{ .Values.greenmqtt.dataDir | quote }}
{{- if .Values.greenmqtt.stateEndpoint }}
- name: GREENMQTT_STATE_ENDPOINT
  value: {{ .Values.greenmqtt.stateEndpoint | quote }}
{{- else if .Values.storeTopology.enabled }}
- name: GREENMQTT_STATE_ENDPOINT
  value: {{ printf "http://%s:%v" (include "greenmqtt.storeFullname" .) (.Values.storeTopology.service.rpc.port | int) | quote }}
{{- end }}
{{- if .Values.greenmqtt.redisUrl }}
- name: GREENMQTT_REDIS_URL
  value: {{ .Values.greenmqtt.redisUrl | quote }}
{{- end }}
{{- if .Values.greenmqtt.responseInformation }}
- name: GREENMQTT_RESPONSE_INFORMATION
  value: {{ .Values.greenmqtt.responseInformation | quote }}
{{- end }}
{{- if .Values.greenmqtt.serverReference }}
- name: GREENMQTT_SERVER_REFERENCE
  value: {{ .Values.greenmqtt.serverReference | quote }}
{{- end }}
{{- if ne .Values.greenmqtt.serverKeepAliveSecs nil }}
- name: GREENMQTT_SERVER_KEEP_ALIVE_SECS
  value: {{ .Values.greenmqtt.serverKeepAliveSecs | quote }}
{{- end }}
{{- if ne .Values.greenmqtt.maxPacketSize nil }}
- name: GREENMQTT_MAX_PACKET_SIZE
  value: {{ .Values.greenmqtt.maxPacketSize | quote }}
{{- end }}
{{- range $name, $value := .Values.greenmqtt.extraEnv }}
- name: {{ $name }}
  value: {{ $value | quote }}
{{- end }}
{{- end -}}

{{/*
Rendered store environment variables.
*/}}
{{- define "greenmqtt.storeEnv" -}}
- name: GREENMQTT_RPC_BIND
  value: {{ .Values.storeTopology.greenmqtt.rpcBind | quote }}
- name: GREENMQTT_STORAGE_BACKEND
  value: {{ .Values.storeTopology.greenmqtt.storageBackend | quote }}
- name: GREENMQTT_DATA_DIR
  value: {{ .Values.storeTopology.greenmqtt.dataDir | quote }}
{{- range $name, $value := .Values.storeTopology.greenmqtt.extraEnv }}
- name: {{ $name }}
  value: {{ $value | quote }}
{{- end }}
{{- end -}}
