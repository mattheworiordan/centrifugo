package configtypes

// Ably is a configuration for the Ably protocol adapter. When enabled, Centrifugo
// serves the Ably client wire protocol (realtime WebSocket + REST) so unmodified
// Ably SDKs can connect to it. The adapter claims the web root of the external
// HTTP server because Ably SDKs construct root-relative paths (/time,
// /channels/..., realtime WebSocket at /) which cannot be prefixed. EXPERIMENTAL.
type Ably struct {
	Enabled  bool   `mapstructure:"enabled" json:"enabled" envconfig:"enabled" yaml:"enabled" toml:"enabled" doc:"Enables the Ably protocol adapter (experimental). Serves Ably REST and realtime WebSocket endpoints at the web root of the external HTTP server."`
	KeysFile string `mapstructure:"keys_file" json:"keys_file" envconfig:"keys_file" yaml:"keys_file" toml:"keys_file" doc:"Path to a static Ably app fixture JSON file providing the app's API keys and capabilities for the Ably protocol adapter (experimental, PoC-only)."`
}
