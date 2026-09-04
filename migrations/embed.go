// Package migrations embeds the SQL schema migrations into the binary.
//
// Embedding rather than shipping a directory means a distroless image with a
// single static binary can migrate itself, and that a migration can never be out
// of sync with the code that expects it.
//
// The package lives alongside the .sql files because go:embed cannot reach outside
// its own directory.
package migrations

import "embed"

// FS holds every migration file. Filenames follow golang-migrate's convention:
// NNNN_name.up.sql applies a change, NNNN_name.down.sql reverses it.
//
// Down migrations exist and are tested. That is not because a production rollback
// is likely -- it rarely is for a schema with data in it -- but because a
// migration you cannot reverse is one you cannot test cheaply, and the integration
// suite runs up/down/up.
//
//go:embed *.sql
var FS embed.FS
