package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/dsub-io/go-open-discogs-batch/src/batch"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const Prefix = "OPEN_DISCOGS_BATCH_"

const versionPrintPrefix = "go-open-discogs-batch"

var version string
var conf = koanf.New(".")
var runBatch = func(ctx context.Context, config *koanf.Koanf, releaseVersion string) error {
	return (&batch.Runner{Version: releaseVersion}).Run(ctx, config)
}
var loadEnvironmentConfig = loadEnvironment
var loadFlagConfig = func(flags *pflag.FlagSet, k *koanf.Koanf) error {
	return k.Load(posflag.Provider(flags, ".", k), nil)
}
var normalizeEntityConfig = func(k *koanf.Koanf) error { return normalizeEntities(k) }
var setEntityListConfig = func(k *koanf.Koanf, entities []string) error {
	return k.Set("entities", entities)
}
var setEntitySelectionConfig = func(k *koanf.Koanf, key string, selected bool) error {
	return k.Set(key, selected)
}

var environmentOptionNames = map[string]string{
	"DATABASE_URL":    "database-url",
	"ENTITIES":        "entities",
	"DUMP_MONTH":      "dump-month",
	"DATA_DIR":        "data-dir",
	"CHUNK_SIZE":      "chunk-size",
	"MAX_WORKERS":     "max-workers",
	"CLEANUP":         "cleanup",
	"FORCE":           "force",
	"ALLOW_DOWNGRADE": "allow-downgrade",
	"DATABASE_SCHEMA": "database-schema",
}

var booleanEnvironmentOptions = map[string]struct{}{
	"CLEANUP":         {},
	"FORCE":           {},
	"ALLOW_DOWNGRADE": {},
}

func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return NewRootCommand().ExecuteContext(ctx)
}

func NewRootCommand() *cobra.Command {
	conf = koanf.New(".")
	rootCmd := &cobra.Command{
		Use:          "go-open-discogs-batch",
		Short:        "Import OpenDiscogs data dumps into PostgreSQL",
		SilenceUsage: true,
		Long: `go-open-discogs-batch imports selected OpenDiscogs data dumps
into the canonical PostgreSQL schema published by open-discogs-model.`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return load(cmd.Flags(), conf)
		},
		RunE: getMainFunc(),
	}

	dataDir := filepath.Join(getHomeDir(new(homeDirSupplier)), ".cache", "open-discogs-batch")
	f := rootCmd.Flags()
	f.String("database-url", "", "PostgreSQL URI including credentials (required)")
	f.String("database-schema", database.DefaultSchemaName, "PostgreSQL schema for canonical tables")
	f.StringSliceP("entities", "e", []string{"artist", "label", "master", "release"}, "entities to import")
	f.StringP("dump-month", "m", "", "exact dump month in yyyy-MM form (default: latest per entity)")
	f.String("data-dir", dataDir, "download directory")
	f.IntP("chunk-size", "b", 5000, "import chunk size")
	f.Int("max-workers", runtime.GOMAXPROCS(0), "maximum concurrent import workers")
	f.BoolP("cleanup", "c", false, "delete downloads after a successful import")
	f.BoolP("force", "f", false, "reprocess an already successful dump")
	f.Bool("allow-downgrade", false, "allow an older dump than the entity checkpoint")
	f.BoolP("version", "v", false, "print version")
	return rootCmd
}

var getMainFunc = func() func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if ok, _ := cmd.Flags().GetBool("version"); ok {
			fmt.Println(versionPrintPrefix, printableVersion())
			return nil
		}
		if err := new(validator).Validate(conf); err != nil {
			return err
		}
		return runBatch(cmd.Context(), conf, version)
	}
}

func printableVersion() string {
	if strings.TrimSpace(version) == "" {
		return "development"
	}
	return version
}

func load(flags *pflag.FlagSet, k *koanf.Koanf) error {
	if ok, _ := flags.GetBool("version"); ok {
		return nil
	}
	if err := loadEnvironmentConfig(k); err != nil {
		return err
	}
	if err := loadFlagConfig(flags, k); err != nil {
		return err
	}
	return normalizeEntityConfig(k)
}

func loadEnvironment(k *koanf.Koanf) error {
	return k.Load(
		env.ProviderWithValue(Prefix, ".", func(key string, value string) (string, interface{}) {
			suffix := strings.TrimPrefix(key, Prefix)
			name, ok := environmentOptionNames[suffix]
			if !ok {
				return "", nil
			}
			if suffix == "ENTITIES" {
				return name, splitValues(value)
			}
			if _, isBoolean := booleanEnvironmentOptions[suffix]; isBoolean {
				switch strings.ToLower(strings.TrimSpace(value)) {
				case "true", "1", "yes", "on":
					return name, true
				case "false", "0", "no", "off":
					return name, false
				default:
					return name, value
				}
			}
			return name, value
		}),
		nil,
	)
}

func normalizeEntities(k *koanf.Koanf) error {
	entities := make([]string, 0, len(k.Strings("entities")))
	selected := make(map[string]bool)
	for _, entity := range k.Strings("entities") {
		normalized := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(entity)), "s")
		if normalized == "" || selected[normalized] {
			continue
		}
		selected[normalized] = true
		entities = append(entities, normalized)
	}
	if err := setEntityListConfig(k, entities); err != nil {
		return err
	}
	for _, entity := range []string{"artist", "label", "master", "release"} {
		if err := setEntitySelectionConfig(k, entity+"s", selected[entity]); err != nil {
			return err
		}
	}
	return nil
}

func splitValues(value string) []string {
	values := strings.Split(value, ",")
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}
	return values
}

func getHomeDir(supplier HomeDirSupplier) string {
	if home, err := supplier.HomeUserDir(); err != nil {
		panic("failed to determine home directory")
	} else {
		return home
	}
}

type HomeDirSupplier interface {
	HomeUserDir() (string, error)
}

type homeDirSupplier struct{}

func (h *homeDirSupplier) HomeUserDir() (string, error) {
	return os.UserHomeDir()
}
