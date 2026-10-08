// Package docs holds the published machine contracts of the platform.
//
// The JSON files in this directory are the single hand-maintained source of
// those contracts. The Go services embed them at build time (see
// internal/contracts) instead of rebuilding an equivalent document in code, so
// what /openapi.json and /schemas/mqtt-envelope.json serve can never drift from
// what this directory publishes. Edit the JSON files here, not a Go literal.
package docs

import (
	"bytes"
	_ "embed"
)

//go:embed openapi.json
var openAPISpec []byte

//go:embed mqtt-envelope.schema.json
var mqttEnvelopeSchema []byte

// OpenAPISpecJSON returns the contents of docs/openapi.json. A copy is returned
// so callers cannot mutate the embedded document.
func OpenAPISpecJSON() []byte {
	return bytes.Clone(openAPISpec)
}

// MQTTEnvelopeSchemaJSON returns the contents of docs/mqtt-envelope.schema.json.
// A copy is returned so callers cannot mutate the embedded document.
func MQTTEnvelopeSchemaJSON() []byte {
	return bytes.Clone(mqttEnvelopeSchema)
}
