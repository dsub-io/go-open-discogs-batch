package cmd

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func executeWithoutBatch(t *testing.T, args ...string) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
	cmd.SetArgs(args)
	require.NoError(t, cmd.Execute())
}

func TestPublicDefaults(t *testing.T) {
	executeWithoutBatch(t)

	require.Equal(t, []string{"artist", "label", "master", "release"}, conf.Strings("entities"))
	require.Equal(t, 5000, conf.Int("chunk-size"))
	require.Equal(t, filepath.Join(getHomeDir(new(homeDirSupplier)), ".cache", "open-discogs-batch"), conf.String("data-dir"))
	require.False(t, conf.Bool("cleanup"))
}

func TestEnvironmentVariablesAndCommandLinePrecedence(t *testing.T) {
	t.Setenv("OPEN_DISCOGS_BATCH_DATABASE_URL", "postgresql://env:pass@db:5432/discogs")
	t.Setenv("OPEN_DISCOGS_BATCH_ENTITIES", "artist, release")
	t.Setenv("OPEN_DISCOGS_BATCH_DUMP_MONTH", "2026-07")
	t.Setenv("OPEN_DISCOGS_BATCH_DATA_DIR", "/env-data")
	t.Setenv("OPEN_DISCOGS_BATCH_CHUNK_SIZE", "3500")
	t.Setenv("OPEN_DISCOGS_BATCH_CLEANUP", "true")
	t.Setenv("OPEN_DISCOGS_BATCH_FORCE", "true")
	t.Setenv("OPEN_DISCOGS_BATCH_ALLOW_DOWNGRADE", "true")

	executeWithoutBatch(
		t,
		"--database-url", "postgresql://cli:pass@db:5432/discogs",
		"--entities", "label,master",
		"--chunk-size", "5500",
		"--data-dir", "/cli-data",
	)

	require.Equal(t, "postgresql://cli:pass@db:5432/discogs", conf.String("database-url"))
	require.Equal(t, []string{"label", "master"}, conf.Strings("entities"))
	require.Equal(t, "2026-07", conf.String("dump-month"))
	require.Equal(t, "/cli-data", conf.String("data-dir"))
	require.Equal(t, 5500, conf.Int("chunk-size"))
	require.True(t, conf.Bool("cleanup"))
	require.True(t, conf.Bool("force"))
	require.True(t, conf.Bool("allow-downgrade"))
	require.False(t, conf.Bool("artists"))
	require.True(t, conf.Bool("labels"))
	require.True(t, conf.Bool("masters"))
	require.False(t, conf.Bool("releases"))
}

func TestLegacyFlagsAreRejected(t *testing.T) {
	for _, legacy := range []string{"--config", "--dsn", "--types", "--year", "--month", "--purge", "--new", "--update"} {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{legacy})
		require.Error(t, cmd.Execute(), legacy)
	}
}

func TestVersionDoesNotRequireDatabaseURL(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"--version"})
	require.NoError(t, cmd.Execute())
}

func TestGetHomeDir(t *testing.T) {
	require.NotEmpty(t, getHomeDir(new(homeDirSupplier)))
}

type failingHomeDirSupplier struct{}

func (failingHomeDirSupplier) HomeUserDir() (string, error) {
	return "", errors.New("test error")
}

func TestGetHomeDirPanics(t *testing.T) {
	require.Panics(t, func() { getHomeDir(failingHomeDirSupplier{}) })
}
