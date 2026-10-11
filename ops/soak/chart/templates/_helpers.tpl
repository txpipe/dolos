{{- define "dolos-soak.name" -}}
{{ required "name is required" .Values.name }}
{{- end -}}

{{- define "dolos-soak.configMapName" -}}
{{ include "dolos-soak.name" . }}-config
{{- end -}}

{{/*
Pinned by digest: a soak judges one exact build, and a tag can move under it.
*/}}
{{- define "dolos-soak.image" -}}
{{- $digest := required "image.digest is required" .Values.image.digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail (printf "image.digest must be sha256:<64 hex>, got %q" $digest) -}}
{{- end -}}
{{ required "image.repository is required" .Values.image.repository }}@{{ $digest }}
{{- end -}}

{{- define "dolos-soak.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ include "dolos-soak.name" . }}
{{- end -}}

{{- define "dolos-soak.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{ include "dolos-soak.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
The latest block is at most freshness.maxAgeSeconds old. The image ships no
HTTP client, so bash's /dev/tcp makes the request; nothing is installed at
container start.
*/}}
{{- define "dolos-soak.freshnessCheck" -}}
exec:
  command:
    - /bin/bash
    - -c
    - |
      set -eu
      request='exec 3<>"/dev/tcp/127.0.0.1/$1" && printf "GET /blocks/latest HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n" >&3 && cat <&3'
      response="$(timeout 3 bash -c "$request" request {{ int .Values.freshness.port }})"
      case "$response" in "HTTP/1."?" 200"*) ;; *) exit 1 ;; esac
      block_time="$(printf '%s' "$response" | grep -oE '"time"[[:space:]]*:[[:space:]]*[0-9]+' | head -n 1 | grep -oE '[0-9]+$' || true)"
      case "$block_time" in ''|*[!0-9]*) exit 1 ;; esac
      now="$(date +%s)"
      [ "$((now - block_time))" -le {{ int .Values.freshness.maxAgeSeconds }} ]
{{- end -}}
