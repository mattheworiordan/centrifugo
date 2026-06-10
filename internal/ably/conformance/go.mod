// Standalone module: the Ably SDK conformance smoke imports ably-go, which
// must never become a dependency of the centrifugo binary. Run via
// `make ably-conformance` against a live adapter (ABLY_CONFORMANCE_URL).
module github.com/centrifugal/centrifugo/v6/internal/ably/conformance

go 1.24

require (
	github.com/ably/ably-go v1.4.1
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/ably/vcdiff-go v0.0.2 // indirect
	github.com/coder/websocket v1.8.12 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/ugorji/go/codec v1.1.9 // indirect
	golang.org/x/sys v0.2.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
