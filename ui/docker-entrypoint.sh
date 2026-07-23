#!/bin/sh
# Runs during nginx container startup (via /docker-entrypoint.d) BEFORE nginx
# launches. Writes the runtime config the SPA reads as window.__API_URL__, from
# the API_URL env var (set by Helm values). The nginx.conf template itself is
# rendered separately by the image's built-in 20-envsubst step.
set -eu

: "${API_URL:=/api}"

cat > /usr/share/nginx/html/config.js <<EOF
window.__API_URL__ = "${API_URL}";
EOF

echo "deep-research: wrote /config.js with window.__API_URL__=\"${API_URL}\""
