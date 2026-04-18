package database

import "embed"

// MigrationFiles contains the goose SQL migration files for notasap_jobs.
// Callers apply migrations using goose v3:
//
//	goose.SetBaseFS(database.MigrationFiles)
//	goose.Up(db, "migrations")
//
//go:embed migrations/*.sql
var MigrationFiles embed.FS
