# The pool service. Build from the pool directory:
#
#   docker build -f docker/Dockerfile -t tztcloud/openpool:0.1.0 .
#
# Everything it keeps is under /data: the SQLite database and the key that
# signs compose files. Mount a volume there; losing it loses the pool's books
# and makes every member install the agent again.
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
# SQLite is compiled in, which needs cgo.
RUN CGO_ENABLED=1 go build -trimpath -ldflags "-s -w" -o /out/openpool ./cmd/openpool

FROM debian:bookworm-slim
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/* \
	&& useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin openpool \
	&& mkdir /data && chown openpool:openpool /data
COPY --from=build /out/openpool /usr/local/bin/openpool
USER openpool
WORKDIR /data
ENV OPENPOOL_LISTEN=0.0.0.0:8090 \
	OPENPOOL_DB_PATH=/data/openpool.db \
	OPENPOOL_COMPOSE_KEY_FILE=/data/openpool-compose.key
VOLUME /data
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/openpool"]
