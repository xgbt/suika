FROM golang:1.25 AS builder

COPY . /src
WORKDIR /src

# cgo is required by mattn/go-sqlite3; gcc ships with the golang image.
RUN GOPROXY=https://goproxy.cn make build

FROM debian:stable-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
		ca-certificates \
		netbase \
		&& rm -rf /var/lib/apt/lists/ \
		&& apt-get autoremove -y && apt-get autoclean -y

COPY --from=builder /src/bin /app

WORKDIR /app

EXPOSE 8000
EXPOSE 9000

# Mount your config directory here (config.yaml + optional credentials.yaml).
VOLUME /data/conf

# Runtime data lives relative to WORKDIR: sqlite in ./data/suika.db and
# recordings in ./recordings — mount volumes at /app/data and
# /app/recordings to persist them.
CMD ["./suika", "-conf", "/data/conf"]
