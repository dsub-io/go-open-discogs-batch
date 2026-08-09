package cmd

import (
	"testing"
	"time"

	"github.com/knadh/koanf"
	"github.com/stretchr/testify/require"
)

func TestValidDatabaseURL(t *testing.T) {
	for _, value := range []string{
		"postgresql://user:pass@host:5432/db_name?sslmode=require",
		"postgres://user:p%40ss@host/db_name",
	} {
		require.NoError(t, ValidDatabaseURL(value))
	}
	for _, value := range []string{
		"",
		"mysql://user:pass@host:3306/db",
		"postgresql://host:5432/db",
		"postgresql://user:pass@host:5432",
	} {
		require.Error(t, ValidDatabaseURL(value))
	}
}

func TestValidEntities(t *testing.T) {
	require.NoError(t, ValidEntities([]string{"artist", "labels", "master", "release"}))
	require.ErrorContains(t, ValidEntities(nil), "must not be empty")
	require.ErrorContains(t, ValidEntities([]string{"artist", "unknown"}), "unknown")
}

func TestValidChunkSize(t *testing.T) {
	for _, value := range []string{"-1", "0", "x", ""} {
		require.Error(t, ValidChunkSize(value))
	}
	require.NoError(t, ValidChunkSize("1"))
	require.NoError(t, ValidChunkSize("327564344"))
	require.Error(t, ValidChunkSize("2147483648"))
}

func TestValidMaxWorkers(t *testing.T) {
	for _, value := range []string{"-1", "0", "x", "", "2147483648"} {
		require.Error(t, ValidMaxWorkers(value))
	}
	require.NoError(t, ValidMaxWorkers("1"))
	require.NoError(t, ValidMaxWorkers("32"))
}

func TestValidDumpMonth(t *testing.T) {
	require.NoError(t, ValidDumpMonth(""))
	require.NoError(t, ValidDumpMonth("2008-03"))
	for _, value := range []string{"2008-02", "2026-7", "2026-00", "2026-13", "invalid"} {
		require.Error(t, ValidDumpMonth(value))
	}
	require.Error(t, ValidDumpMonth(time.Now().UTC().AddDate(0, 1, 0).Format("2006-01")))
}

func TestValidator(t *testing.T) {
	config := koanf.New(".")
	require.NoError(t, config.Set("database-url", "postgresql://user:pass@db:5432/discogs"))
	require.NoError(t, config.Set("entities", []string{"artist"}))
	require.NoError(t, config.Set("chunk-size", 5000))
	require.NoError(t, config.Set("max-workers", 4))
	require.NoError(t, config.Set("dump-month", "2026-07"))

	require.NoError(t, new(validator).Validate(config))

	require.NoError(t, config.Set("entities", []string{"unknown"}))
	require.ErrorContains(t, new(validator).Validate(config), "unknown entities")
}
