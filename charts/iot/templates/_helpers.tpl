{{- define "iot.labels" -}}
app.kubernetes.io/name: iot
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: Helm
{{- end -}}
