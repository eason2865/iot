{{- define "iot.mqttContainer" -}}
- name: {{ .name }}
  image: {{ .root.Values.images.app }}
  imagePullPolicy: IfNotPresent
  command: ["/usr/local/bin/{{ .name }}"]
  securityContext:
    allowPrivilegeEscalation: false
    capabilities:
      drop:
        - ALL
  resources:
    {{- toYaml .resources | nindent 4 }}
  envFrom:
    - configMapRef:
        name: iot-common-config
    - secretRef:
        name: {{ .root.Values.security.existingSecret }}
  env:
    - name: EMQX_CLIENT_ID_SUFFIX
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
  volumeMounts:
    - name: runtime-tmp
      mountPath: /tmp
  ports:
    # The single HTTP port serves /healthz, /readyz and /metrics. It is
    # named "metrics" so Prometheus kubernetes_sd can select it by port
    # name and scrape every Pod individually.
    - name: metrics
      containerPort: 8080
  startupProbe:
    httpGet:
      path: /healthz
      port: 8080
    periodSeconds: 5
    failureThreshold: 30
  readinessProbe:
    httpGet:
      path: /readyz
      port: 8080
    initialDelaySeconds: 5
    periodSeconds: 5
  livenessProbe:
    httpGet:
      path: /healthz
      port: 8080
    initialDelaySeconds: 15
    periodSeconds: 10
{{- end -}}
