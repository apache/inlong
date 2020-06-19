{{/*
Define the tubemq broker
*/}}
{{- define "tubemq.broker.service" -}}
{{ template "tubemq.fullname" . }}-{{ .Values.broker.component }}
{{- end }}

{{/*
Define the broker hostname
*/}}
{{- define "tubemq.broker.hostname" -}}
${HOSTNAME}.{{ template "tubemq.broker.service" . }}.{{ .Release.Namespace }}.svc.cluster.local
{{- end -}}