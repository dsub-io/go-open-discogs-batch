package batch

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/dsub-io/go-open-discogs-batch/src/database"
	opendiscogsschema "github.com/dsub-io/open-discogs-model/schema"
	"gorm.io/gorm"
)

const (
	liquibaseLockTable          = "databasechangeloglock"
	liquibaseChangeLogTable     = "databasechangelog"
	liquibaseLockID             = 1
	liquibaseMigrationLockOwner = "go-open-discogs-batch canonical migrator"
	legacyChangeSetIDPrefix     = "open-discogs-model-v"
	postgresqlVersionMajorScale = 10_000
)

var liquibaseChecksumPattern = regexp.MustCompile(`^9:[0-9a-f]{32}$`)

var (
	loadLegacyLiquibaseCompatibility = opendiscogsschema.LegacyLiquibaseCompatibility
	loadLegacyLiquibaseContract      = opendiscogsschema.LegacyLiquibaseContract
)

type liquibaseMigrationLock struct {
	db       *gorm.DB
	schema   database.Schema
	acquired bool
}

type legacyLiquibaseRow struct {
	ID            string
	Author        string
	Filename      string
	ExecutionType string
	Checksum      string
}

func acquireLiquibaseMigrationLock(
	db *gorm.DB,
	schema database.Schema,
) (*liquibaseMigrationLock, error) {
	lock := &liquibaseMigrationLock{db: db, schema: schema}
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(fmt.Sprintf(`
			create table if not exists %s
			(
			    id          integer not null primary key,
			    locked      boolean not null,
			    lockgranted timestamp,
			    lockedby    varchar(255)
			)`, schema.Qualify(liquibaseLockTable))).Error; err != nil {
			return fmt.Errorf("create Liquibase migration lock table: %w", err)
		}
		if err := tx.Exec(
			"insert into "+schema.Qualify(liquibaseLockTable)+
				" (id, locked) values (?, false) on conflict (id) do nothing",
			liquibaseLockID,
		).Error; err != nil {
			return fmt.Errorf("initialize Liquibase migration lock: %w", err)
		}
		var locked bool
		result := tx.Raw(
			"select locked from "+schema.Qualify(liquibaseLockTable)+
				" where id = ? for update nowait",
			liquibaseLockID,
		).Scan(&locked)
		if result.Error != nil {
			return fmt.Errorf("reserve Liquibase migration lock: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("Liquibase migration lock row %d is missing", liquibaseLockID)
		}
		if locked {
			return fmt.Errorf(
				"Liquibase migration lock is already held in schema %s; stop the other migrator or verify and clear a stale lock",
				schema.Name(),
			)
		}
		result = tx.Exec(
			"update "+schema.Qualify(liquibaseLockTable)+
				" set locked = true, lockgranted = now(), lockedby = ? where id = ? and not locked",
			liquibaseMigrationLockOwner,
			liquibaseLockID,
		)
		if result.Error != nil {
			return fmt.Errorf("acquire Liquibase migration lock: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("Liquibase migration lock changed while being acquired")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	lock.acquired = true
	return lock, nil
}

func (lock *liquibaseMigrationLock) release() error {
	if lock == nil || !lock.acquired {
		return nil
	}
	result := lock.db.Exec(
		"update "+lock.schema.Qualify(liquibaseLockTable)+
			" set locked = false, lockgranted = null, lockedby = null where id = ? and locked and lockedby = ?",
		liquibaseLockID,
		liquibaseMigrationLockOwner,
	)
	if result.Error != nil {
		return fmt.Errorf("release Liquibase migration lock: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("Liquibase migration lock ownership changed before release")
	}
	lock.acquired = false
	return nil
}

func adoptLegacyLiquibaseHistory(
	db *gorm.DB,
	schema database.Schema,
	canonical []canonicalMigration,
) error {
	manifest, err := loadLegacyLiquibaseCompatibility()
	if err != nil {
		return fmt.Errorf("load legacy Liquibase compatibility contract: %w", err)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"lock table " + schema.Qualify(migrationTable) + " in exclusive mode",
		).Error; err != nil {
			return fmt.Errorf("lock shared migration ledger for legacy adoption: %w", err)
		}

		var ledgerCount int64
		if err := tx.Raw(
			"select count(*) from " + schema.Qualify(migrationTable),
		).Scan(&ledgerCount).Error; err != nil {
			return fmt.Errorf("count shared migration ledger: %w", err)
		}
		rows, err := readLegacyLiquibaseRows(tx, schema)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		if ledgerCount > 0 {
			if ledgerCount < int64(len(rows)) {
				return fmt.Errorf(
					"shared migration ledger has %d rows but legacy Liquibase history has %d; refusing partial adoption",
					ledgerCount,
					len(rows),
				)
			}
			return nil
		}

		legacyMigrations, requiresSchemaProof, err := validateLegacyLiquibaseRows(
			manifest,
			schema,
			canonical,
			rows,
		)
		if err != nil {
			return err
		}
		if requiresSchemaProof {
			if err := verifyLegacySchemaContract(tx, schema, manifest, len(rows)); err != nil {
				return err
			}
		}
		for _, migration := range legacyMigrations {
			if err := tx.Exec(
				"insert into "+schema.Qualify(migrationTable)+" (version, checksum) values (?, ?)",
				migration.CanonicalFilename,
				migration.CanonicalSHA256,
			).Error; err != nil {
				return fmt.Errorf("adopt legacy migration %s: %w", migration.Version, err)
			}
		}
		return nil
	})
}

func readLegacyLiquibaseRows(
	tx *gorm.DB,
	schema database.Schema,
) ([]legacyLiquibaseRow, error) {
	var exists bool
	if err := tx.Raw(
		"select exists(select 1 from information_schema.tables where table_schema = ? and table_name = ?)",
		schema.Name(),
		liquibaseChangeLogTable,
	).Scan(&exists).Error; err != nil {
		return nil, fmt.Errorf("inspect legacy Liquibase history: %w", err)
	}
	if !exists {
		return nil, nil
	}
	var rows []legacyLiquibaseRow
	result := tx.Raw(
		`select id, author, filename, exectype as execution_type, md5sum as checksum
		   from `+schema.Qualify(liquibaseChangeLogTable)+`
		  where id like ?
		  order by id, author, filename`,
		legacyChangeSetIDPrefix+"%",
	).Scan(&rows)
	if result.Error != nil {
		return nil, fmt.Errorf("read legacy Liquibase history: %w", result.Error)
	}
	return rows, nil
}

func validateLegacyLiquibaseRows(
	manifest opendiscogsschema.LegacyLiquibaseManifest,
	schema database.Schema,
	canonical []canonicalMigration,
	rows []legacyLiquibaseRow,
) ([]opendiscogsschema.LegacyLiquibaseMigration, bool, error) {
	prefix, found := manifest.SchemaContract(fmt.Sprintf("V%03d", len(rows)))
	if !found {
		return nil, false, fmt.Errorf("legacy Liquibase history length %d is not a supported release prefix", len(rows))
	}
	if len(rows) > len(canonical) || len(prefix.MigrationVersions) != len(rows) {
		return nil, false, fmt.Errorf("legacy Liquibase history is not a canonical artifact prefix")
	}
	wantedMode := opendiscogsschema.LegacySchemaModeCustom
	if schema.Name() == database.DefaultSchemaName {
		wantedMode = opendiscogsschema.LegacySchemaModePublic
	}
	requiresSchemaProof := false
	validated := make([]opendiscogsschema.LegacyLiquibaseMigration, 0, len(rows))
	for index, row := range rows {
		migration := manifest.Migrations[index]
		if migration.CanonicalFilename != canonical[index].version ||
			migration.CanonicalSHA256 != canonical[index].checksum {
			return nil, false, fmt.Errorf("legacy migration %s does not match the canonical artifact", migration.Version)
		}
		var expected *opendiscogsschema.LegacyChangeSet
		for changeSetIndex := range migration.LegacyChangeSets {
			candidate := &migration.LegacyChangeSets[changeSetIndex]
			if candidate.SchemaMode == wantedMode {
				expected = candidate
				break
			}
		}
		if expected == nil || row.ID != expected.ID || row.Author != expected.Author || row.Filename != expected.Filename {
			return nil, false, fmt.Errorf("legacy migration %s has an untrusted Liquibase identity", migration.Version)
		}
		policy, permitted := expected.ExecutionPolicy(
			opendiscogsschema.LegacyExecutionType(row.ExecutionType),
		)
		if !permitted {
			return nil, false, fmt.Errorf("legacy migration %s has unsupported execution type %q", migration.Version, row.ExecutionType)
		}
		if expected.ChecksumPolicy == opendiscogsschema.LegacyChecksumPolicyExact {
			if row.Checksum != expected.Checksum {
				return nil, false, fmt.Errorf("legacy migration %s Liquibase checksum changed", migration.Version)
			}
		} else if !liquibaseChecksumPattern.MatchString(row.Checksum) {
			return nil, false, fmt.Errorf("legacy migration %s has an invalid parameterized checksum", migration.Version)
		}
		if policy.AdoptionProof == opendiscogsschema.LegacyAdoptionProofSchemaContract {
			requiresSchemaProof = true
		}
		validated = append(validated, migration)
	}
	return validated, requiresSchemaProof, nil
}

func verifyLegacySchemaContract(
	tx *gorm.DB,
	schema database.Schema,
	manifest opendiscogsschema.LegacyLiquibaseManifest,
	prefixLength int,
) error {
	contract, found := manifest.SchemaContract(fmt.Sprintf("V%03d", prefixLength))
	if !found {
		return fmt.Errorf("legacy schema contract for prefix V%03d is unavailable", prefixLength)
	}
	var serverVersionNumber int
	if err := tx.Raw("show server_version_num").Scan(&serverVersionNumber).Error; err != nil {
		return fmt.Errorf("read PostgreSQL version for legacy schema verification: %w", err)
	}
	postgresqlMajor := serverVersionNumber / postgresqlVersionMajorScale
	expected, found := contract.ExpectedFingerprint(postgresqlMajor)
	if !found {
		return fmt.Errorf("legacy schema verification does not support PostgreSQL %d", postgresqlMajor)
	}
	verifier, err := loadLegacyLiquibaseContract(contract.Verifier.Name)
	if err != nil {
		return fmt.Errorf("load legacy schema verifier: %w", err)
	}
	if err := tx.Exec("set local search_path to " + schema.SearchPath()).Error; err != nil {
		return fmt.Errorf("select legacy schema for verification: %w", err)
	}
	var fingerprintInput sql.NullString
	if err := tx.Raw(string(verifier)).Scan(&fingerprintInput).Error; err != nil {
		return fmt.Errorf("calculate legacy schema fingerprint: %w", err)
	}
	if !fingerprintInput.Valid {
		return fmt.Errorf("legacy schema fingerprint query returned null")
	}
	digest := sha256.Sum256([]byte(fingerprintInput.String))
	actual := hex.EncodeToString(digest[:])
	if !strings.EqualFold(actual, expected) {
		return fmt.Errorf("legacy schema fingerprint mismatch: database=%s contract=%s", actual, expected)
	}
	return nil
}
