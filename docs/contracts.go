// Package docs holds the published machine contracts of the platform.
//
// The JSON files in this directory are the single hand-maintained source of
// those contracts. The Go services embed them at build time (see
// internal/contracts) instead of rebuilding an equivalent document in code, so
// what /schemas/mqtt-envelope.json serves can never drift from what this
// directory publishes. Edit the JSON file here, not a Go literal.
package docs

import (
	"bytes"
	_ "embed"
)

//go:embed mqtt-envelope.schema.json
var mqttEnvelopeSchema []byte

// MQTTEnvelopeSchemaJSON returns the contents of docs/mqtt-envelope.schema.json.
// A copy is returned so callers cannot mutate the embedded document.
func MQTTEnvelopeSchemaJSON() []byte {
	return bytes.Clone(mqttEnvelopeSchema)
}
