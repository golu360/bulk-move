// Package migrations embeds the SQL migration files so tests can run them
// through golang-migrate's iofs source without shelling out. Production
// binaries apply migrations via the golang-migrate CLI (see the Makefile),
// never through this package.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
