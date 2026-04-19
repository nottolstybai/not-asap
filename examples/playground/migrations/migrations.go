package migrations

import "embed"

// Files contains all playground SQL migration files applied in lexical order.
// It includes both the notasap library schema and the playground-specific orders table.
//
//go:embed *.sql
var Files embed.FS
