{{/*
Define the tubemq zookeeper
*/}}
{{- define "tubemq.zookeeper.service" -}}
{{ template "tubemq.fullname" . }}-{{ .Values.zookeeper.component }}
{{- end }}

{{/*
Define the zookeeper hostname
*/}}
{{- define "tubemq.zookeeper.hostname" -}}
${HOSTNAME}.{{ template "tubemq.zookeeper.service" . }}.{{ .Release.Namespace }}.svc.cluster.local
{{- end -}}