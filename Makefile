VERSION := $(shell git describe --tags | sed -e 's/^v//g' | awk -F "-" '{print $$1}')
ITERATION := $(shell git describe --tags --long | awk -F "-" '{print $$2}')
TESTFOLDERS := $(shell go list ./... | grep -v /misc/)

all: test

test:
	go test -count=1 -v $(TESTFOLDERS) -cover -race

test-integration:
	go test -count=1 -v $(TESTFOLDERS) -cover -race --tags=integration

generate:
	bash misc/scripts/generate.sh
	go generate ./...

web:
	./misc/scripts/update_web.sh

swagger-web:
	make generate
	./misc/scripts/update_swagger_web.sh

package:
	./misc/scripts/package.sh $(VERSION) $(ITERATION)

sync-websocket:
	rsync -av --exclude '*/' ../centrifuge/internal/websocket/ internal/websocket/

packagecloud:
	make packagecloud-deb
	make packagecloud-rpm

packagecloud-deb:
	# PACKAGECLOUD_TOKEN env must be set
	package_cloud push FZambia/centrifugo/debian/buster PACKAGES/*.deb
	package_cloud push FZambia/centrifugo/debian/bullseye PACKAGES/*.deb
	package_cloud push FZambia/centrifugo/debian/bookworm PACKAGES/*.deb

	package_cloud push FZambia/centrifugo/ubuntu/focal PACKAGES/*.deb
	package_cloud push FZambia/centrifugo/ubuntu/jammy PACKAGES/*.deb
	package_cloud push FZambia/centrifugo/ubuntu/noble PACKAGES/*.deb

packagecloud-rpm:
	# PACKAGECLOUD_TOKEN env must be set
	package_cloud push FZambia/centrifugo/el/7 PACKAGES/*.rpm

update-deps:
	./misc/scripts/update-deps.sh

deps:
	go mod tidy

local-deps:
	go mod tidy
	go mod download
	go mod vendor

GOLANGCI_LINT_VERSION := $(shell cat .golangci-lint-version)

lint:
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "golangci-lint not found. Run 'make install-lint' to install $(GOLANGCI_LINT_VERSION)."; \
		exit 1; \
	fi
	@INSTALLED=$$(golangci-lint version --short 2>/dev/null); \
	EXPECTED=$$(echo $(GOLANGCI_LINT_VERSION) | sed 's/^v//'); \
	if [ "$${INSTALLED%%.*}" != "$${EXPECTED%%.*}" ] || [ "$${INSTALLED#*.}" != "$${EXPECTED#*.}" ]; then \
		echo "golangci-lint version mismatch: installed $${INSTALLED}, expected $${EXPECTED}. Run 'make install-lint'."; \
		exit 1; \
	fi
	golangci-lint run --timeout 10m0s --verbose

install-lint:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

vuln:
	@if ! command -v govulncheck >/dev/null 2>&1; then \
		echo "govulncheck not found, installing..."; \
		go install golang.org/x/vuln/cmd/govulncheck@latest; \
	fi
	govulncheck ./...

build:
	CGO_ENABLED=0 go build

ably-dev:
	go run . --config config.ably-dev.json

# Run the ably-go conformance smoke (internal/ably/conformance, a standalone
# module) against a freshly built binary serving the ably dev profile. The
# trap kills the server on success and failure alike.
ably-conformance:
	mkdir -p tmp
	go build -o tmp/centrifugo-ably .
	@set -e; \
	./tmp/centrifugo-ably --config config.ably-dev.json > tmp/ably-conformance-server.log 2>&1 & \
	SERVER_PID=$$!; \
	trap 'kill $$SERVER_PID 2>/dev/null || true' EXIT; \
	echo "waiting for adapter on :8049 (pid $$SERVER_PID)"; \
	for i in $$(seq 1 50); do \
		curl -fsS -o /dev/null http://localhost:8049/time 2>/dev/null && break; \
		kill -0 $$SERVER_PID 2>/dev/null || { echo "server exited early; tail of log:"; tail -20 tmp/ably-conformance-server.log; exit 1; }; \
		sleep 0.2; \
	done; \
	curl -fsS -o /dev/null http://localhost:8049/time; \
	cd internal/ably/conformance && ABLY_CONFORMANCE_URL=localhost:8049 go test -count=1 -timeout 120s -v ./...


# ably-poc-test is the PoC Definition-of-Done gate: the native Go suites,
# the ably-go conformance mirrors, and the full ably-js acceptance sweep
# (the allowlisted suites, non-comet) against a freshly built binary.
# Requires node + the pinned ably-js checkout (.working/ably-js-pinned,
# `npm run build:node` already done). ~15 minutes.
ably-poc-test:
	go test ./internal/ably/... -count=1 -timeout 300s
	$(MAKE) ably-conformance
	./scripts/ably-poc-acceptance.sh
