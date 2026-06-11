# Ably-on-Centrifugo PoC — fly.io image.
#
# Build context is the REPO ROOT (the root fly.toml points here), so the
# whole module is available to the builder stage. fly's remote builders
# run this; no local docker needed.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/rt-server .

FROM alpine:3.22
RUN apk --no-cache upgrade && \
    apk --no-cache add ca-certificates && \
    update-ca-certificates && \
    addgroup -S app && adduser -S -G app app

COPY deploy/ably-poc/config.json /etc/rt/config.json
COPY deploy/ably-poc/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY --from=build /out/rt-server /usr/local/bin/rt-server

USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
