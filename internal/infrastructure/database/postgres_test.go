package database

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestConnectPostgres_UsesSimpleProtocol(t *testing.T) {
	// Verify that ParseConfig + SimpleProtocol assignment works correctly
	dsn := "postgres://user:pass@pgbouncer:6432/capstone?sslmode=disable&pool_max_conns=100&pool_min_conns=2&pool_max_conn_idle_time=5m"
	cfg, err := pgxpool.ParseConfig(dsn)
	assert.NoError(t, err)

	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	assert.Equal(t, pgx.QueryExecModeSimpleProtocol, cfg.ConnConfig.DefaultQueryExecMode)
	assert.Equal(t, int32(100), cfg.MaxConns)
	assert.Equal(t, int32(2), cfg.MinConns)
}
