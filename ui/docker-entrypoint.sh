#!/bin/sh
# Runs during nginx container startup (via /docker-entrypoint.d) BEFORE nginx
# launches. Writes the runtime config the SPA reads as window.__API_URL__ (from
# the API_URL env, set by Helm), emits a diagnostics banner, and probes the
# backend so `kubectl logs deploy/<release>-ui` shows whether the apiserver is
# reachable from inside this pod. The nginx.conf template is rendered separately
# by the image's built-in 20-envsubst step.
set -eu

: "${API_URL:=/api}"
: "${APISERVER_UPSTREAM:=http://deep-research-apiserver:8123}"

# 1) Runtime config the browser reads (window.__API_URL__).
cat > /usr/share/nginx/html/config.js <<EOF
window.__API_URL__ = "${API_URL}";
EOF

# 2) Machine-readable diagnostics served at /__debug (see nginx.conf.template).
cat > /usr/share/nginx/html/__debug.json <<EOF
{"api_url":"${API_URL}","apiserver_upstream":"${APISERVER_UPSTREAM}","pod":"$(hostname)"}
EOF

echo "=================================================================="
echo "deep-research UI container config:"
echo "  API_URL (browser window.__API_URL__)      = ${API_URL}"
echo "  APISERVER_UPSTREAM (nginx /api/ proxy dst) = ${APISERVER_UPSTREAM}"
echo "  pod                                        = $(hostname)"
echo "=================================================================="

# 3) Probe the backend from inside this pod (does NOT fail startup if it's down).
#    busybox wget ships in nginx:alpine; -T is the timeout, -O - writes to stdout.
i=1
while [ "$i" -le 3 ]; do
  if body="$(wget -q -T 3 -O - "${APISERVER_UPSTREAM}/health" 2>/dev/null)"; then
    echo "deep-research: backend health OK  ${APISERVER_UPSTREAM}/health -> ${body}"
    break
  fi
  echo "deep-research: backend NOT reachable at ${APISERVER_UPSTREAM}/health (attempt ${i}/3)"
  i=$((i + 1))
  [ "$i" -le 3 ] && sleep 1
done

echo "deep-research: startup diagnostics done; handing off to nginx"
