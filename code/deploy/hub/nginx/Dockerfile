# TLS front door + console static files. Build context = repo root.
FROM node:22-bookworm-slim@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da AS console
WORKDIR /console
COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY console/ ./
RUN npm run build

FROM nginx:1.27-alpine@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10
LABEL org.opencontainers.image.title="alelt-proxy" \
      org.opencontainers.image.authors="Freddy Beltran"
COPY --from=console /console/dist /usr/share/nginx/html
COPY deploy/hub/nginx/nginx.conf /etc/nginx/nginx.conf
COPY deploy/hub/nginx/alelt-strip-identity.conf /etc/nginx/alelt-strip-identity.conf
EXPOSE 443
