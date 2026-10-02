# One of the three sites as an image: its built files behind a small web
# server. Build from pool/web, naming the site:
#
#   docker build -f docker/Dockerfile --build-arg SITE=member -t tztcloud/openpool-web-member:0.1.0 .
#
# SITE is marketing, member or admin. The image holds no config.json: mount
# the one that names your regions at /usr/share/nginx/html/config.json.
#
# The public site's pages carry addresses fixed when it is built:
#
#   --build-arg OPENPOOL_SITE_URL=https://open-pool.com --build-arg OPENPOOL_MEMBER_URL=https://app.open-pool.com
FROM node:24-bookworm-slim AS build
ARG SITE
ARG OPENPOOL_SITE_URL=https://open-pool.com
ARG OPENPOOL_MEMBER_URL=https://app.open-pool.com
ENV OPENPOOL_SITE_URL=${OPENPOOL_SITE_URL} OPENPOOL_MEMBER_URL=${OPENPOOL_MEMBER_URL} CI=true
RUN test -n "${SITE}" || (echo "build with --build-arg SITE=marketing|member|admin" && false)
RUN corepack enable
WORKDIR /src
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.base.json ./
COPY packages ./packages
COPY apps ./apps
COPY e2e/package.json ./e2e/package.json
RUN pnpm install --frozen-lockfile --filter "@openpool/${SITE}..."
RUN pnpm --filter "@openpool/${SITE}" build \
	&& rm -rf "apps/${SITE}/dist/config.json" "apps/${SITE}/dist/.vite" \
	&& find "apps/${SITE}/dist" -name '*.map' -delete

FROM nginxinc/nginx-unprivileged:1.29-alpine
ARG SITE
COPY docker/nginx-${SITE}.conf /etc/nginx/conf.d/default.conf
COPY --from=build /src/apps/${SITE}/dist /usr/share/nginx/html
EXPOSE 8080
