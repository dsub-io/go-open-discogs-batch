package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/dsub-io/go-open-discogs-batch/src/batch"
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
}

var booleanEnvironmentOptions = map[string]struct{}{
	"CLEANUP":         {},
	"FORCE":           {},
	"ALLOW_DOWNGRADE": {},
}

func Execute() error {
	return NewRootCommand().Execute()
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
		return (&batch.Runner{Version: version}).Run(context.Background(), conf)
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
	if err := loadEnvironment(k); err != nil {
		return err
	}
	if err := k.Load(posflag.Provider(flags, ".", k), nil); err != nil {
		return err
	}
	return normalizeEntities(k)
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
	if err := k.Set("entities", entities); err != nil {
		return err
	}
	for _, entity := range []string{"artist", "label", "master", "release"} {
		if err := k.Set(entity+"s", selected[entity]); err != nil {
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
