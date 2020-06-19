{{/*
Define the tubemq master
*/}}
{{- define "tubemq.master.service" -}}
{{ template "tubemq.fullname" . }}-{{ .Values.master.component }}
{{- end }}

{{/*
Define the master hostname
*/}}
{{- define "tubemq.master.hostname" -}}
${HOSTNAME}.{{ template "tubemq.master.service" . }}.{{ .Release.Namespace }}.svc.cluster.local
{{- end -}}