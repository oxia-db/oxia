# Copyright 2023-2025 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

{{/*
Coordinator labels
*/}}
{{- define "oxia-cluster.coordinator.labels" -}}
{{ include "oxia-cluster.coordinator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/part-of: oxia
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Coordinator selector labels
*/}}
{{- define "oxia-cluster.coordinator.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/component: coordinator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Server labels
*/}}
{{- define "oxia-cluster.server.labels" -}}
{{ include "oxia-cluster.server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Server selector labels
*/}}
{{- define "oxia-cluster.server.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/component: server
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Probe
*/}}
{{- define "oxia-cluster.probe" -}}
exec:
  command: ["oxia", "health", "--port={{ . }}"]
initialDelaySeconds: 10
timeoutSeconds: 10
{{- end }}


{{/*
Probe
*/}}
{{- define "oxia-cluster.readiness-probe" -}}
exec:
  command: ["oxia", "health", "--port={{ . }}", "--service=oxia-readiness"]
initialDelaySeconds: 10
timeoutSeconds: 10
{{- end }}

{{/*
Probe
*/}}
{{- define "oxia-cluster.startup-probe" -}}
exec:
  command: ["oxia", "health", "--port={{ . }}"]
initialDelaySeconds: 60
timeoutSeconds: 10
{{- end }}

