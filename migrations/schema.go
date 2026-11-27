// Package migrations owns the SQL used by both deployment tooling and runtime bootstrap.
package migrations

import _ "embed"

//go:embed 001_init.sql
var InitialSQL string
