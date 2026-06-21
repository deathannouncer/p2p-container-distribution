# --- build stage ---
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/node ./cmd/node

# --- runtime stage ---
# debian-slim (not distroless) because scripts/entrypoint.sh needs a shell
# to derive NODE_ID/PEERS from the StatefulSet pod ordinal in k8s.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/node /node
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
VOLUME ["/var/lib/p2pcd/data"]
EXPOSE 8080
ENTRYPOINT ["/entrypoint.sh"]
