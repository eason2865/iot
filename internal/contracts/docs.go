package contracts

import (
	"encoding/json"

	"iot/docs"
)

// MQTTEnvelopeSchema returns the JSON Schema for MQTT telemetry envelopes.
//
// The document is not built here: docs/mqtt-envelope.schema.json is the single
// hand-maintained source and is embedded at build time, so the schema served at
// /schemas/mqtt-envelope.json and the published file are always the same
// document. Edit the JSON file, never a Go literal.
//
// A fresh map is decoded on each call so callers cannot corrupt the shared
// document by mutating the result. The embedded JSON is validated by
// TestMQTTEnvelopeSchemaIsTheEmbeddedPublishedFile, so a malformed document
// fails the test suite rather than this function.
func MQTTEnvelopeSchema() map[string]any {
	return decodeEmbeddedSpec(docs.MQTTEnvelopeSchemaJSON())
}

// decodeEmbeddedSpec decodes an embedded contract document. It never panics: a
// malformed document yields nil, which the schema endpoint renders as an empty
// object. Tests assert the document decodes, so this is a runtime safety net
// rather than the primary check.
func decodeEmbeddedSpec(raw []byte) map[string]any {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return doc
}
