package contracts

import (
	"encoding/json"

	"iot/docs"
)

// OpenAPISpec returns the OpenAPI 3.1 specification for the management REST API.
//
// The document is not built here: docs/openapi.json is the single
// hand-maintained source and is embedded at build time, so the spec served at
// /openapi.json and the published file are always the same document. Edit the
// JSON file, never a Go literal.
//
// A fresh map is decoded on each call so callers cannot corrupt the shared
// document by mutating the result. The embedded JSON is validated by
// TestOpenAPISpecIsTheEmbeddedPublishedFile, so a malformed document fails the
// test suite rather than this function.
func OpenAPISpec() map[string]any {
	return decodeEmbeddedSpec(docs.OpenAPISpecJSON())
}

// MQTTEnvelopeSchema returns the JSON Schema for MQTT telemetry envelopes. Like
// OpenAPISpec it is embedded from docs/mqtt-envelope.schema.json, which is the
// single hand-maintained source.
func MQTTEnvelopeSchema() map[string]any {
	return decodeEmbeddedSpec(docs.MQTTEnvelopeSchemaJSON())
}

// decodeEmbeddedSpec decodes an embedded contract document. It never panics: a
// malformed document yields nil, which the docs endpoints render as an empty
// object. Tests assert the documents decode, so this is a runtime safety net
// rather than the primary check.
func decodeEmbeddedSpec(raw []byte) map[string]any {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	return doc
}
