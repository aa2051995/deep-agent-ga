# syntax=docker/dockerfile:1
# ---- Build stage: compile the Vite/React SPA ----
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

# ---- Runtime stage: nginx serves the static bundle and reverse-proxies the API ----
FROM nginx:1.27-alpine
# gettext provides `envsubst`, used by the entrypoint to template config at boot.
RUN apk add --no-cache gettext

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY docker-entrypoint.sh /docker-entrypoint.d/40-deep-agent-ga-config.sh
RUN chmod +x /docker-entrypoint.d/40-deep-agent-ga-config.sh

# Same-origin by default: the browser calls /api/*, nginx proxies it to the
# apiserver Service inside the cluster. Overridable via Helm values.
ENV API_URL=/api \
    APISERVER_UPSTREAM=http://deep-agent-ga-apiserver:8123

EXPOSE 8080
