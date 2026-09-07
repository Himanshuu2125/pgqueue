// Package migrations embeds the SQL schema and applies it with golang-migrate.
package migrations

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed *.sql
var FS embed.FS

// Up applies all migrations to databaseURL (postgres:// form). Used by cmd/api and tests.
func Up(databaseURL string) error {
	src, err := iofs.New(FS, ".")
	if err != nil {
		return fmt.Errorf("migrations source: %w", err)
	}
	url := "pgx5://" + strings.TrimPrefix(strings.TrimPrefix(databaseURL, "postgres://"), "postgresql://")
	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return fmt.Errorf("migrate init: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
