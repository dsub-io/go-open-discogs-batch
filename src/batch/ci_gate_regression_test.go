package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dsub-io/go-open-discogs-batch/src/database"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	opendiscogsschema "github.com/dsub-io/open-discogs-model/schema"
	"github.com/stretchr/testify/require"
)

func TestNullableRelationDeleteErrorPreservesNilKeyBoundary(t *testing.T) {
	expected := errors.New("fixture")
	db := poisonedGorm(t, expected)
	item := &opendiscogsmodel.LabelReleaseItem{
		ReleaseItemID:    1,
		LabelID:          2,
		CategoryNotation: nil,
	}
	actual := reconcileIntegerNullableTextKeyRelation(
		NewOrder(context.Background(), 1, 1, "unused", db),
		labelReleaseItemRelation,
		true,
		[]int32{item.ReleaseItemID},
		[]*opendiscogsmodel.LabelReleaseItem{item},
		func(value *opendiscogsmodel.LabelReleaseItem) int32 { return value.ReleaseItemID },
		func(value *opendiscogsmodel.LabelReleaseItem) int32 { return value.LabelID },
		func(value *opendiscogsmodel.LabelReleaseItem) *string { return value.CategoryNotation },
	)
	require.ErrorIs(t, actual.Err(), expected)
}

func TestRunDDLPropagatesLiquibaseAndLegacyAdoptionFailures(t *testing.T) {
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	expected := errors.New("fixture")

	t.Run("Liquibase lock", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		keys := expectAllMigrationLocks(t, mock)
		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta(
			"create table if not exists " + schema.Qualify(liquibaseLockTable),
		)).WillReturnError(expected)
		mock.ExpectRollback()
		expectAllMigrationUnlocks(mock, keys)

		require.ErrorContains(
			t,
			runDDL(db, migrationFixture("select 1"), schema),
			"create Liquibase migration lock table",
		)
	})

	t.Run("legacy contract", func(t *testing.T) {
		originalLoad := loadLegacyLiquibaseCompatibility
		loadLegacyLiquibaseCompatibility = func() (
			opendiscogsschema.LegacyLiquibaseManifest,
			error,
		) {
			return opendiscogsschema.LegacyLiquibaseManifest{}, expected
		}
		t.Cleanup(func() { loadLegacyLiquibaseCompatibility = originalLoad })

		db, mock, _ := newMockGorm(t)
		keys := expectAllMigrationLocks(t, mock)
		expectLiquibaseMigrationLock(mock, schema)
		expectMigrationLedger(t, mock, schema)
		expectLiquibaseMigrationUnlock(mock, schema)
		expectAllMigrationUnlocks(mock, keys)

		require.ErrorContains(
			t,
			runDDL(db, migrationFixture("select 1"), schema),
			"load legacy Liquibase compatibility contract",
		)
	})
}

func TestLiquibaseMigrationLockErrorBoundaries(t *testing.T) {
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	expected := errors.New("fixture")
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{
			name: "create table",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("create table if not exists").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "create Liquibase migration lock table",
		},
		{
			name: "initialize row",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("create table if not exists").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into .*databasechangeloglock").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "initialize Liquibase migration lock",
		},
		{
			name: "reserve row",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("create table if not exists").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into .*databasechangeloglock").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("select locked from .*databasechangeloglock").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "reserve Liquibase migration lock",
		},
		{
			name: "missing row",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("create table if not exists").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into .*databasechangeloglock").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("select locked from .*databasechangeloglock").
					WillReturnRows(sqlmock.NewRows([]string{"locked"}))
				mock.ExpectRollback()
			},
			want: "lock row 1 is missing",
		},
		{
			name: "already locked",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("create table if not exists").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into .*databasechangeloglock").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("select locked from .*databasechangeloglock").
					WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
				mock.ExpectRollback()
			},
			want: "already held",
		},
		{
			name: "acquire update",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("create table if not exists").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into .*databasechangeloglock").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("select locked from .*databasechangeloglock").
					WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(false))
				mock.ExpectExec("update .*databasechangeloglock").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "acquire Liquibase migration lock",
		},
		{
			name: "acquire race",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("create table if not exists").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("insert into .*databasechangeloglock").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectQuery("select locked from .*databasechangeloglock").
					WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(false))
				mock.ExpectExec("update .*databasechangeloglock").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectRollback()
			},
			want: "changed while being acquired",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, _ := newMockGorm(t)
			test.setup(mock)
			_, err := acquireLiquibaseMigrationLock(db, schema)
			require.ErrorContains(t, err, test.want)
		})
	}

	require.NoError(t, (*liquibaseMigrationLock)(nil).release())
	require.NoError(t, (&liquibaseMigrationLock{}).release())

	t.Run("release query", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectExec("update .*databasechangeloglock").WillReturnError(expected)
		err := (&liquibaseMigrationLock{db: db, schema: schema, acquired: true}).release()
		require.ErrorContains(t, err, "release Liquibase migration lock")
	})

	t.Run("release ownership", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectExec("update .*databasechangeloglock").WillReturnResult(sqlmock.NewResult(0, 0))
		err := (&liquibaseMigrationLock{db: db, schema: schema, acquired: true}).release()
		require.ErrorContains(t, err, "ownership changed before release")
	})
}

func TestReadLegacyLiquibaseRowsErrorBoundaries(t *testing.T) {
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	expected := errors.New("fixture")

	t.Run("inspect", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("select exists.*information_schema.tables").WillReturnError(expected)
		_, err := readLegacyLiquibaseRows(db, schema)
		require.ErrorContains(t, err, "inspect legacy Liquibase history")
	})

	t.Run("read", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("select exists.*information_schema.tables").
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectQuery("select id, author, filename").WillReturnError(expected)
		_, err := readLegacyLiquibaseRows(db, schema)
		require.ErrorContains(t, err, "read legacy Liquibase history")
	})
}

func TestValidateLegacyLiquibaseRowsRejectsUntrustedHistory(t *testing.T) {
	t.Run("unsupported prefix", func(t *testing.T) {
		manifest, canonical, _, schema := legacyValidationFixture(t, database.DefaultSchemaName, 4)
		_, _, err := validateLegacyLiquibaseRows(manifest, schema, canonical, nil)
		require.ErrorContains(t, err, "not a supported release prefix")
	})

	tests := []struct {
		name       string
		schemaName string
		mutate     func(*opendiscogsschema.LegacyLiquibaseManifest, *[]canonicalMigration, *[]legacyLiquibaseRow)
		want       string
	}{
		{
			name:       "canonical prefix length",
			schemaName: database.DefaultSchemaName,
			mutate: func(_ *opendiscogsschema.LegacyLiquibaseManifest, canonical *[]canonicalMigration, _ *[]legacyLiquibaseRow) {
				*canonical = (*canonical)[:3]
			},
			want: "not a canonical artifact prefix",
		},
		{
			name:       "canonical mismatch",
			schemaName: database.DefaultSchemaName,
			mutate: func(_ *opendiscogsschema.LegacyLiquibaseManifest, canonical *[]canonicalMigration, _ *[]legacyLiquibaseRow) {
				(*canonical)[0].checksum = "changed"
			},
			want: "does not match the canonical artifact",
		},
		{
			name:       "missing schema mode",
			schemaName: database.DefaultSchemaName,
			mutate: func(manifest *opendiscogsschema.LegacyLiquibaseManifest, _ *[]canonicalMigration, _ *[]legacyLiquibaseRow) {
				manifest.Migrations[0].LegacyChangeSets = nil
			},
			want: "untrusted Liquibase identity",
		},
		{
			name:       "identity",
			schemaName: database.DefaultSchemaName,
			mutate: func(_ *opendiscogsschema.LegacyLiquibaseManifest, _ *[]canonicalMigration, rows *[]legacyLiquibaseRow) {
				(*rows)[0].Author = "other"
			},
			want: "untrusted Liquibase identity",
		},
		{
			name:       "execution type",
			schemaName: database.DefaultSchemaName,
			mutate: func(_ *opendiscogsschema.LegacyLiquibaseManifest, _ *[]canonicalMigration, rows *[]legacyLiquibaseRow) {
				(*rows)[0].ExecutionType = "FAILED"
			},
			want: "unsupported execution type",
		},
		{
			name:       "exact checksum",
			schemaName: database.DefaultSchemaName,
			mutate: func(_ *opendiscogsschema.LegacyLiquibaseManifest, _ *[]canonicalMigration, rows *[]legacyLiquibaseRow) {
				(*rows)[0].Checksum = "9:00000000000000000000000000000000"
			},
			want: "Liquibase checksum changed",
		},
		{
			name:       "parameterized checksum",
			schemaName: "open_discogs",
			mutate: func(_ *opendiscogsschema.LegacyLiquibaseManifest, _ *[]canonicalMigration, rows *[]legacyLiquibaseRow) {
				(*rows)[0].Checksum = "invalid"
			},
			want: "invalid parameterized checksum",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest, canonical, rows, schema := legacyValidationFixture(t, test.schemaName, 4)
			test.mutate(&manifest, &canonical, &rows)
			_, _, err := validateLegacyLiquibaseRows(manifest, schema, canonical, rows)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestVerifyLegacySchemaContractErrorBoundaries(t *testing.T) {
	expected := errors.New("fixture")
	manifest, _, _, schema := legacyValidationFixture(t, database.DefaultSchemaName, 4)
	contract, found := manifest.SchemaContract("V004")
	require.True(t, found)
	verifier, err := opendiscogsschema.LegacyLiquibaseContract(contract.Verifier.Name)
	require.NoError(t, err)

	t.Run("missing prefix", func(t *testing.T) {
		db, _, _ := newMockGorm(t)
		require.ErrorContains(
			t,
			verifyLegacySchemaContract(db, schema, opendiscogsschema.LegacyLiquibaseManifest{}, 4),
			"contract for prefix V004 is unavailable",
		)
	})

	t.Run("server version", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("show server_version_num").WillReturnError(expected)
		require.ErrorContains(t, verifyLegacySchemaContract(db, schema, manifest, 4), "read PostgreSQL version")
	})

	t.Run("unsupported PostgreSQL", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("show server_version_num").
			WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow(190000))
		require.ErrorContains(t, verifyLegacySchemaContract(db, schema, manifest, 4), "does not support PostgreSQL 19")
	})

	t.Run("verifier resource", func(t *testing.T) {
		broken := manifest
		broken.SchemaContracts = append(
			[]opendiscogsschema.LegacySchemaContract(nil),
			manifest.SchemaContracts...,
		)
		broken.SchemaContracts[0].Verifier.Name = "missing.sql"
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("show server_version_num").
			WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow(180000))
		require.ErrorContains(t, verifyLegacySchemaContract(db, schema, broken, 4), "load legacy schema verifier")
	})

	t.Run("search path", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		mock.ExpectQuery("show server_version_num").
			WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow(180000))
		mock.ExpectExec("set local search_path").WillReturnError(expected)
		require.ErrorContains(t, verifyLegacySchemaContract(db, schema, manifest, 4), "select legacy schema")
	})

	t.Run("fingerprint query", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		expectLegacyVerifierPrelude(mock)
		mock.ExpectQuery(regexp.QuoteMeta(string(verifier))).WillReturnError(expected)
		require.ErrorContains(t, verifyLegacySchemaContract(db, schema, manifest, 4), "calculate legacy schema fingerprint")
	})

	t.Run("null fingerprint", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		expectLegacyVerifierPrelude(mock)
		mock.ExpectQuery(regexp.QuoteMeta(string(verifier))).
			WillReturnRows(sqlmock.NewRows([]string{"fingerprint"}).AddRow(nil))
		require.ErrorContains(t, verifyLegacySchemaContract(db, schema, manifest, 4), "returned null")
	})

	t.Run("fingerprint mismatch", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		expectLegacyVerifierPrelude(mock)
		mock.ExpectQuery(regexp.QuoteMeta(string(verifier))).
			WillReturnRows(sqlmock.NewRows([]string{"fingerprint"}).AddRow("fixture"))
		require.ErrorContains(t, verifyLegacySchemaContract(db, schema, manifest, 4), "fingerprint mismatch")
	})
}

func TestAdoptLegacyLiquibaseHistoryErrorBoundaries(t *testing.T) {
	schema := requireMigrationSchema(t, database.DefaultSchemaName)
	expected := errors.New("fixture")
	manifest, canonical, rows, _ := legacyValidationFixture(t, database.DefaultSchemaName, 4)
	require.NotEmpty(t, manifest.Migrations)

	t.Run("load contract", func(t *testing.T) {
		originalLoad := loadLegacyLiquibaseCompatibility
		loadLegacyLiquibaseCompatibility = func() (opendiscogsschema.LegacyLiquibaseManifest, error) {
			return opendiscogsschema.LegacyLiquibaseManifest{}, expected
		}
		t.Cleanup(func() { loadLegacyLiquibaseCompatibility = originalLoad })
		require.ErrorContains(t, adoptLegacyLiquibaseHistory(nil, schema, canonical), "load legacy")
	})

	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  string
	}{
		{
			name: "ledger lock",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("lock table .*open_discogs_schema_migration").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "lock shared migration ledger",
		},
		{
			name: "ledger count",
			setup: func(mock sqlmock.Sqlmock) {
				expectLegacyAdoptionStart(mock)
				mock.ExpectQuery("select count.*open_discogs_schema_migration").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "count shared migration ledger",
		},
		{
			name: "history inspection",
			setup: func(mock sqlmock.Sqlmock) {
				expectLegacyAdoptionStart(mock)
				mock.ExpectQuery("select count.*open_discogs_schema_migration").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
				mock.ExpectQuery("select exists.*information_schema.tables").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "inspect legacy Liquibase history",
		},
		{
			name: "partial ledger",
			setup: func(mock sqlmock.Sqlmock) {
				expectLegacyAdoptionStart(mock)
				mock.ExpectQuery("select count.*open_discogs_schema_migration").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
				expectLegacyRows(mock, schema, rows)
				mock.ExpectRollback()
			},
			want: "refusing partial adoption",
		},
		{
			name: "validation",
			setup: func(mock sqlmock.Sqlmock) {
				expectLegacyAdoptionStart(mock)
				mock.ExpectQuery("select count.*open_discogs_schema_migration").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
				expectLegacyRows(mock, schema, rows[:1])
				mock.ExpectRollback()
			},
			want: "not a supported release prefix",
		},
		{
			name: "adopt insert",
			setup: func(mock sqlmock.Sqlmock) {
				expectLegacyAdoptionStart(mock)
				mock.ExpectQuery("select count.*open_discogs_schema_migration").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
				expectLegacyRows(mock, schema, rows)
				mock.ExpectExec("insert into .*open_discogs_schema_migration").WillReturnError(expected)
				mock.ExpectRollback()
			},
			want: "adopt legacy migration V001",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, _ := newMockGorm(t)
			test.setup(mock)
			require.ErrorContains(t, adoptLegacyLiquibaseHistory(db, schema, canonical), test.want)
		})
	}

	t.Run("existing complete ledger", func(t *testing.T) {
		db, mock, _ := newMockGorm(t)
		expectLegacyAdoptionStart(mock)
		mock.ExpectQuery("select count.*open_discogs_schema_migration").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))
		expectLegacyRows(mock, schema, rows)
		mock.ExpectCommit()
		require.NoError(t, adoptLegacyLiquibaseHistory(db, schema, canonical))
	})

	t.Run("schema proof verification failure", func(t *testing.T) {
		proofRows := append([]legacyLiquibaseRow(nil), rows...)
		proofRows[0].ExecutionType = string(opendiscogsschema.LegacyExecutionTypeMarkRan)
		db, mock, _ := newMockGorm(t)
		expectLegacyAdoptionStart(mock)
		mock.ExpectQuery("select count.*open_discogs_schema_migration").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		expectLegacyRows(mock, schema, proofRows)
		mock.ExpectQuery("show server_version_num").WillReturnError(expected)
		mock.ExpectRollback()

		require.ErrorContains(
			t,
			adoptLegacyLiquibaseHistory(db, schema, canonical),
			"read PostgreSQL version for legacy schema verification",
		)
	})

	t.Run("schema proof adoption", func(t *testing.T) {
		const fingerprint = "fixture"
		digest := sha256.Sum256([]byte(fingerprint))
		schemaProofManifest := manifest
		schemaProofManifest.SchemaContracts = append(
			[]opendiscogsschema.LegacySchemaContract(nil),
			manifest.SchemaContracts...,
		)
		for contractIndex := range schemaProofManifest.SchemaContracts {
			contract := &schemaProofManifest.SchemaContracts[contractIndex]
			if contract.Prefix != "V004" {
				continue
			}
			contract.ExpectedFingerprints = append(
				[]opendiscogsschema.LegacySchemaFingerprint(nil),
				contract.ExpectedFingerprints...,
			)
			for fingerprintIndex := range contract.ExpectedFingerprints {
				if contract.ExpectedFingerprints[fingerprintIndex].PostgreSQLMajor == 18 {
					contract.ExpectedFingerprints[fingerprintIndex].SHA256 = hex.EncodeToString(digest[:])
				}
			}
		}

		proofRows := append([]legacyLiquibaseRow(nil), rows...)
		proofRows[0].ExecutionType = string(opendiscogsschema.LegacyExecutionTypeMarkRan)
		originalCompatibilityLoader := loadLegacyLiquibaseCompatibility
		originalContractLoader := loadLegacyLiquibaseContract
		loadLegacyLiquibaseCompatibility = func() (opendiscogsschema.LegacyLiquibaseManifest, error) {
			return schemaProofManifest, nil
		}
		loadLegacyLiquibaseContract = func(string) ([]byte, error) {
			return []byte("select 'fixture'"), nil
		}
		t.Cleanup(func() {
			loadLegacyLiquibaseCompatibility = originalCompatibilityLoader
			loadLegacyLiquibaseContract = originalContractLoader
		})

		db, mock, _ := newMockGorm(t)
		expectLegacyAdoptionStart(mock)
		mock.ExpectQuery("select count.*open_discogs_schema_migration").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		expectLegacyRows(mock, schema, proofRows)
		expectLegacyVerifierPrelude(mock)
		mock.ExpectQuery(regexp.QuoteMeta("select 'fixture'")).
			WillReturnRows(sqlmock.NewRows([]string{"fingerprint"}).AddRow(fingerprint))
		for range canonical {
			mock.ExpectExec("insert into .*open_discogs_schema_migration").
				WillReturnResult(sqlmock.NewResult(0, 1))
		}
		mock.ExpectCommit()
		require.NoError(t, adoptLegacyLiquibaseHistory(db, schema, canonical))
	})
}

func TestImportCoordinatorContractResolutionAndConsolidationFailures(t *testing.T) {
	t.Run("resolve entity revision", func(t *testing.T) {
		original, found := currentImportContractRevisions[artistEntityType]
		require.True(t, found)
		delete(currentImportContractRevisions, artistEntityType)
		t.Cleanup(func() { currentImportContractRevisions[artistEntityType] = original })

		coordinator := NewImportExecutionCoordinator(nil, "version")
		_, err := coordinator.Prepare(
			context.Background(),
			[]*opendiscogsmodel.DiscogsDump{importDump(artistEntityType, "2026-07-01", "a")},
			5,
			false,
			false,
		)
		require.ErrorContains(t, err, "contract revision is unavailable")
	})

	t.Run("consolidate successful checkpoints", func(t *testing.T) {
		expected := errors.New("fixture")
		dump := importDump(artistEntityType, "2026-07-01", "a")
		_, mock, sqlDB := newMockGorm(t)
		expectEntityLock(mock)
		mock.ExpectBegin()
		expectMarkAbandoned(mock)
		expectNoCheckpoint(mock)
		expectInsertedDump(mock, dump)
		mock.ExpectQuery("select candidate_run.id").WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{
				"id", "compatible_entity_count", "selected_entity_count", "incompatible_entity_types",
			}).AddRow(nil, 1, 1, ""))
		mock.ExpectQuery("insert into discogs_import_run").
			WithArgs(sqlmock.AnyArg(), processorName, "version").
			WillReturnError(expected)
		mock.ExpectRollback()
		expectEntityUnlock(mock)

		_, err := NewImportExecutionCoordinator(sqlDB, "version").Prepare(
			context.Background(),
			[]*opendiscogsmodel.DiscogsDump{dump},
			5,
			false,
			false,
		)
		require.ErrorContains(t, err, "record consolidated successful import run")
	})
}

func legacyValidationFixture(
	t *testing.T,
	schemaName string,
	prefixLength int,
) (
	opendiscogsschema.LegacyLiquibaseManifest,
	[]canonicalMigration,
	[]legacyLiquibaseRow,
	database.Schema,
) {
	t.Helper()
	manifest, err := opendiscogsschema.LegacyLiquibaseCompatibility()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(manifest.Migrations), prefixLength)
	schema := requireMigrationSchema(t, schemaName)
	mode := opendiscogsschema.LegacySchemaModeCustom
	if schemaName == database.DefaultSchemaName {
		mode = opendiscogsschema.LegacySchemaModePublic
	}
	canonical := make([]canonicalMigration, prefixLength)
	rows := make([]legacyLiquibaseRow, prefixLength)
	for index, migration := range manifest.Migrations[:prefixLength] {
		canonical[index] = canonicalMigration{
			version:  migration.CanonicalFilename,
			checksum: migration.CanonicalSHA256,
		}
		for _, changeSet := range migration.LegacyChangeSets {
			if changeSet.SchemaMode != mode {
				continue
			}
			checksum := changeSet.Checksum
			if changeSet.ChecksumPolicy == opendiscogsschema.LegacyChecksumPolicySchemaParameterized {
				checksum = "9:00000000000000000000000000000000"
			}
			rows[index] = legacyLiquibaseRow{
				ID:            changeSet.ID,
				Author:        changeSet.Author,
				Filename:      changeSet.Filename,
				ExecutionType: string(opendiscogsschema.LegacyExecutionTypeExecuted),
				Checksum:      checksum,
			}
			break
		}
		require.NotEmpty(t, rows[index].ID)
	}
	return manifest, canonical, rows, schema
}

func expectLegacyAdoptionStart(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec("lock table .*open_discogs_schema_migration").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectLegacyRows(
	mock sqlmock.Sqlmock,
	schema database.Schema,
	rows []legacyLiquibaseRow,
) {
	mock.ExpectQuery("select exists.*information_schema.tables").
		WithArgs(schema.Name(), liquibaseChangeLogTable).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	result := sqlmock.NewRows([]string{"id", "author", "filename", "execution_type", "checksum"})
	for _, row := range rows {
		result.AddRow(row.ID, row.Author, row.Filename, row.ExecutionType, row.Checksum)
	}
	mock.ExpectQuery("select id, author, filename").
		WithArgs(legacyChangeSetIDPrefix + "%").
		WillReturnRows(result)
}

func expectLegacyVerifierPrelude(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("show server_version_num").
		WillReturnRows(sqlmock.NewRows([]string{"server_version_num"}).AddRow(180000))
	mock.ExpectExec("set local search_path").WillReturnResult(sqlmock.NewResult(0, 0))
}
