// Package migrations embeds the notasap schema migrations for goose and golang-migrate.
package migrations

import "embed"

// Goose holds goose migrations under goose/.
//
//go:embed goose/*.sql
var Goose embed.FS

// GolangMigrate holds golang-migrate migrations under golang-migrate/.
//
//go:embed golang-migrate/*.sql
var GolangMigrate embed.FS
