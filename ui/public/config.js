// Dev placeholder so /config.js resolves locally (Vite serves public/ at root).
// In Kubernetes this file is overwritten at container start (docker-entrypoint.sh).
// Leaving __API_URL__ unset makes stream.ts fall back to http://localhost:2024.
window.__API_URL__ = window.__API_URL__ || "";
