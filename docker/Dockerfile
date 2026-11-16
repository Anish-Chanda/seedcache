FROM golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/seedcache ./cmd/seedcache

FROM alpine:3.22
RUN apk add --no-cache ca-certificates openssh-client rsync \
    && addgroup -S seedcache \
    && adduser -S -G seedcache -h /var/lib/seedcache seedcache \
    && mkdir -p /etc/seedcache /var/lib/seedcache \
    && chown -R seedcache:seedcache /var/lib/seedcache

COPY --from=builder /out/seedcache /usr/local/bin/seedcache
EXPOSE 8765
USER seedcache:seedcache
ENTRYPOINT ["/usr/local/bin/seedcache"]
CMD ["--config", "/etc/seedcache/config.yaml", "daemon"]
