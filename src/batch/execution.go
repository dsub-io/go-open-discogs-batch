package batch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	opendiscogsmanifest "github.com/dsub-io/open-discogs-model/manifest"
	opendiscogsmodel "github.com/dsub-io/open-discogs-model/model"
	"github.com/jackc/pgx/v5/pgconn"
)

type importContractRevision int32

const (
	processorName                         = "go-open-discogs-batch"
	artistEntityType                      = "artist"
	labelEntityType                       = "label"
	masterEntityType                      = "master"
	releaseEntityType                     = "release"
	catalogStatusFailed                   = "failed"
	catalogStatusImporting                = "importing"
	catalogStatusReady                    = "ready"
	undefinedColumnSQLState               = "42703"
	importContractRevisionMigration       = "V009"
	importContractRevisionColumnReference = "discogs_import_run_dump.import_contract_revision"
)

var fingerprintImportManifest = opendiscogsmanifest.Fingerprint
var orderImportEntityTypes = opendiscogsmanifest.OrderedEntityTypes
var requiredImportLockTypes = opendiscogsmanifest.RequiredLockEntityTypes
var currentImportContractRevisions = map[string]importContractRevision{
	artistEntityType:  mustImportContractRevision(artistEntityType),
	labelEntityType:   mustImportContractRevision(labelEntityType),
	masterEntityType:  mustImportContractRevision(masterEntityType),
	releaseEntityType: mustImportContractRevision(releaseEntityType),
}

func mustImportContractRevision(entityType string) importContractRevision {
	revision, err := opendiscogsmanifest.ImportContractRevision(entityType)
	if err != nil {
		panic(fmt.Sprintf("load model import contract revision for %s: %v", entityType, err))
	}
	return importContractRevision(revision)
}

type importCheckpointCompatibility struct {
	ExactRunID              int64
	CompatibleEntityCount   int
	SelectedEntityCount     int
	IncompatibleEntityTypes []string
}

type ImportPreparation struct {
	ManifestSHA256   string
	RunID            int64
	ResumedFromRunID int64
	Skipped          bool
}

type ImportExecutionCoordinator struct {
	db               *sql.DB
	processorVersion string

	mu       sync.Mutex
	conn     *sql.Conn
	runID    int64
	lockKeys []int32
	entities []string
}

func NewImportExecutionCoordinator(
	db *sql.DB,
	processorVersion string,
) *ImportExecutionCoordinator {
	if strings.TrimSpace(processorVersion) == "" {
		processorVersion = "development"
	}
	return &ImportExecutionCoordinator{
		db:               db,
		processorVersion: processorVersion,
	}
}

func (c *ImportExecutionCoordinator) Prepare(
	ctx context.Context,
	dumps []*opendiscogsmodel.DiscogsDump,
	chunkSize int,
	force bool,
	allowDowngrade bool,
) (*ImportPreparation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return nil, errors.New("import execution has already been prepared")
	}
	if len(dumps) == 0 {
		return nil, errors.New("import plan must contain at least one dump")
	}
	if chunkSize <= 0 {
		return nil, errors.New("chunk size must be a positive integer")
	}

	manifestDumps := make([]opendiscogsmanifest.Dump, 0, len(dumps))
	entityTypes := make([]string, 0, len(dumps))
	for _, dump := range dumps {
		if dump == nil {
			return nil, errors.New("import plan contains a nil dump")
		}
		manifestDumps = append(manifestDumps, opendiscogsmanifest.Dump{
			EntityType:     dump.EntityType,
			DumpDate:       dump.DumpDate,
			ChecksumSHA256: dump.ChecksumSHA256,
		})
		entityTypes = append(entityTypes, dump.EntityType)
	}

	fingerprint, err := fingerprintImportManifest(manifestDumps)
	if err != nil {
		return nil, fmt.Errorf("fingerprint import manifest: %w", err)
	}
	orderedTypes, err := orderImportEntityTypes(entityTypes)
	if err != nil {
		return nil, err
	}
	revisions, err := resolveImportContractRevisions(entityTypes)
	if err != nil {
		return nil, err
	}
	lockTypes, err := requiredImportLockTypes(orderedTypes)
	if err != nil {
		return nil, err
	}

	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("reserve import lock connection: %w", err)
	}
	c.conn = conn
	if err := c.acquireEntityLocks(ctx, lockTypes); err != nil {
		c.release(ctx)
		return nil, err
	}

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		c.release(ctx)
		return nil, fmt.Errorf("begin import admission transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := markAbandonedRuns(ctx, tx, orderedTypes); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	if err := assertNotDowngrade(ctx, tx, dumps, allowDowngrade); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}

	dumpIDs := make([]int64, len(dumps))
	for index, dump := range dumps {
		dumpIDs[index], err = findOrInsertDump(ctx, tx, dump)
		if err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, err
		}
	}

	compatibility, err := findSuccessfulRun(
		ctx,
		tx,
		fingerprint,
	)
	if err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	if !force && compatibility.SelectedEntityCount == len(dumps) &&
		compatibility.CompatibleEntityCount > 0 &&
		compatibility.CompatibleEntityCount < compatibility.SelectedEntityCount {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, fmt.Errorf(
			"import plan is partially satisfied; preserve compatible checkpoints and rerun only --entities %s",
			strings.Join(compatibility.IncompatibleEntityTypes, ","),
		)
	}
	if !force && compatibility.SelectedEntityCount == len(dumps) &&
		compatibility.CompatibleEntityCount == compatibility.SelectedEntityCount {
		successfulRunID := compatibility.ExactRunID
		if successfulRunID == 0 {
			successfulRunID, err = consolidateSuccessfulImportRun(
				ctx,
				tx,
				fingerprint,
				c.processorVersion,
				len(dumps),
			)
			if err != nil {
				_ = tx.Rollback()
				c.release(ctx)
				return nil, err
			}
		}
		if err := markCatalogStatesReady(
			ctx,
			tx,
			successfulRunID,
			orderedTypes,
		); err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, fmt.Errorf("commit skipped import admission: %w", err)
		}
		committed = true
		c.release(ctx)
		return &ImportPreparation{
			ManifestSHA256: fingerprint,
			RunID:          successfulRunID,
			Skipped:        true,
		}, nil
	}

	resumedFromRunID := int64(0)
	if !force {
		resumedFromRunID, err = findResumableRun(
			ctx,
			tx,
			fingerprint,
			chunkSize,
			len(dumps),
		)
		if err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, err
		}
	}

	runID, err := insertImportRun(
		ctx,
		tx,
		fingerprint,
		force,
		allowDowngrade,
		c.processorVersion,
		resumedFromRunID,
	)
	if err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	for index, dump := range dumps {
		if err := insertImportRunDump(
			ctx,
			tx,
			runID,
			dump.EntityType,
			dumpIDs[index],
			chunkSize,
			revisions[index],
		); err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, err
		}
	}
	if resumedFromRunID != 0 {
		if err := copyResumeProgress(
			ctx,
			tx,
			resumedFromRunID,
			runID,
			len(dumps),
		); err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, err
		}
	}
	if err := markCatalogStatesImporting(ctx, tx, runID, orderedTypes); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, fmt.Errorf("commit import admission: %w", err)
	}
	committed = true
	c.runID = runID
	c.entities = append(c.entities[:0], orderedTypes...)
	return &ImportPreparation{
		ManifestSHA256:   fingerprint,
		RunID:            runID,
		ResumedFromRunID: resumedFromRunID,
	}, nil
}

func (c *ImportExecutionCoordinator) Complete(ctx context.Context, runErr error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil || c.runID == 0 {
		return nil
	}
	defer c.release(ctx)

	tx, err := c.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin import completion transaction: %w", err)
	}
	statusReason := runErr
	var completionErr error
	if statusReason == nil {
		var incompleteEntities int64
		if scanErr := tx.QueryRowContext(
			ctx,
			`select count(*)
			   from discogs_import_run_dump
			  where import_run_id = $1
			    and (completed_at is null
			         or total_items is null
			         or total_chunks is null
			         or processed_items <> total_items)`,
			c.runID,
		).Scan(&incompleteEntities); scanErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("validate import run %d completion: %w", c.runID, scanErr)
		}
		if incompleteEntities != 0 {
			completionErr = fmt.Errorf(
				"import run %d has %d incomplete entities",
				c.runID,
				incompleteEntities,
			)
			statusReason = completionErr
		}
	}

	status := "success"
	failure := sql.NullString{}
	if statusReason != nil {
		status = "failed"
		failure = sql.NullString{String: statusReason.Error(), Valid: true}
	}
	result, err := tx.ExecContext(
		ctx,
		`update discogs_import_run
		    set status = $1,
		        completed_at = now(),
		        failure_message = $2
		  where id = $3
		    and status = 'running'`,
		status,
		failure,
		c.runID,
	)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("complete import run %d: %w", c.runID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("read import completion result: %w", err)
	}
	if affected != 1 {
		_ = tx.Rollback()
		return fmt.Errorf("import run %d was not running", c.runID)
	}
	if statusReason == nil {
		if err := markCatalogStatesReady(ctx, tx, c.runID, c.entities); err != nil {
			_ = tx.Rollback()
			return err
		}
	} else if err := markCatalogStatesFailed(
		ctx,
		tx,
		c.runID,
		c.entities,
		statusReason.Error(),
	); err != nil {
		_ = tx.Rollback()
		return err
	}
	if statusReason == nil {
		if err := pruneSupersededFailedProgress(ctx, tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			"delete from discogs_import_run_chunk where import_run_id = $1",
			c.runID,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("prune completed import run %d chunks: %w", c.runID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit import completion: %w", err)
	}
	c.runID = 0
	return completionErr
}

func (c *ImportExecutionCoordinator) acquireEntityLocks(
	ctx context.Context,
	entityTypes []string,
) error {
	for _, entityType := range entityTypes {
		key, err := opendiscogsmanifest.EntityLockKey(entityType)
		if err != nil {
			return err
		}
		var acquired bool
		if err := c.conn.QueryRowContext(
			ctx,
			"select pg_try_advisory_lock($1, $2)",
			opendiscogsmanifest.AdvisoryLockNamespace,
			key,
		).Scan(&acquired); err != nil {
			return fmt.Errorf("acquire %s import lock: %w", entityType, err)
		}
		if !acquired {
			return fmt.Errorf("another import is already updating %s", entityType)
		}
		c.lockKeys = append(c.lockKeys, key)
	}
	return nil
}

func (c *ImportExecutionCoordinator) release(ctx context.Context) {
	if c.conn == nil {
		return
	}
	for index := len(c.lockKeys) - 1; index >= 0; index-- {
		_, _ = c.conn.ExecContext(
			ctx,
			"select pg_advisory_unlock($1, $2)",
			opendiscogsmanifest.AdvisoryLockNamespace,
			c.lockKeys[index],
		)
	}
	c.lockKeys = nil
	c.entities = nil
	_ = c.conn.Close()
	c.conn = nil
	c.runID = 0
}

func markCatalogStatesImporting(
	ctx context.Context,
	tx *sql.Tx,
	runID int64,
	entityTypes []string,
) error {
	result, err := tx.ExecContext(
		ctx,
		`update discogs_catalog_entity_state
		    set status = $1,
		        operation = case
		            when last_successful_import_run_id is null then 'bootstrap'
		            else 'refresh'
		        end,
		        active_import_run_id = $2,
		        updated_at = now(),
		        failure_message = null
		  where entity_type = any($3::text[])`,
		catalogStatusImporting,
		runID,
		postgresArray(entityTypes),
	)
	return requireCatalogStateTransitions(result, err, len(entityTypes), "start", runID)
}

func markCatalogStatesReady(
	ctx context.Context,
	tx *sql.Tx,
	runID int64,
	entityTypes []string,
) error {
	result, err := tx.ExecContext(
		ctx,
		`update discogs_catalog_entity_state
		    set status = $1,
		        operation = null,
		        active_import_run_id = null,
		        last_successful_import_run_id = $2,
		        ready_at = now(),
		        updated_at = now(),
		        failure_message = null
		  where entity_type = any($3::text[])
		    and (active_import_run_id = $2 or active_import_run_id is null)`,
		catalogStatusReady,
		runID,
		postgresArray(entityTypes),
	)
	return requireCatalogStateTransitions(result, err, len(entityTypes), "finalize", runID)
}

func markCatalogStatesFailed(
	ctx context.Context,
	tx *sql.Tx,
	runID int64,
	entityTypes []string,
	failure string,
) error {
	result, err := tx.ExecContext(
		ctx,
		`update discogs_catalog_entity_state
		    set status = $1,
		        active_import_run_id = null,
		        updated_at = now(),
		        failure_message = $2
		  where entity_type = any($3::text[])
		    and active_import_run_id = $4`,
		catalogStatusFailed,
		failure,
		postgresArray(entityTypes),
		runID,
	)
	return requireCatalogStateTransitions(result, err, len(entityTypes), "fail", runID)
}

func requireCatalogStateTransitions(
	result sql.Result,
	err error,
	expected int,
	action string,
	runID int64,
) error {
	if err != nil {
		return fmt.Errorf("%s import run %d catalog state: %w", action, runID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count %s import run %d catalog states: %w", action, runID, err)
	}
	if affected != int64(expected) {
		return fmt.Errorf(
			"%s import run %d catalog state: updated %d of %d entities",
			action,
			runID,
			affected,
			expected,
		)
	}
	return nil
}

func markAbandonedRuns(
	ctx context.Context,
	tx *sql.Tx,
	entityTypes []string,
) error {
	if _, err := tx.ExecContext(ctx, `
		update discogs_import_run import_run
		   set status = 'failed',
		       completed_at = now(),
		       failure_message = 'recovered after entity advisory locks were released'
		 where import_run.status = 'running'
		   and exists (
		       select 1
		         from discogs_import_run_dump run_dump
		        where run_dump.import_run_id = import_run.id
		          and run_dump.entity_type = any($1::text[])
		   )`, postgresArray(entityTypes)); err != nil {
		return fmt.Errorf("recover abandoned import runs: %w", err)
	}
	return nil
}

func assertNotDowngrade(
	ctx context.Context,
	tx *sql.Tx,
	dumps []*opendiscogsmodel.DiscogsDump,
	allowDowngrade bool,
) error {
	if allowDowngrade {
		return nil
	}
	for _, dump := range dumps {
		var checkpointDate sql.NullTime
		err := tx.QueryRowContext(
			ctx,
			`select dump_date
			   from discogs_import_checkpoint
			  where entity_type = $1`,
			dump.EntityType,
		).Scan(&checkpointDate)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read %s import checkpoint: %w", dump.EntityType, err)
		}
		if checkpointDate.Valid &&
			opendiscogsmanifest.IsDowngrade(dump.DumpDate, checkpointDate.Time) {
			return fmt.Errorf(
				"dump %s %s predates checkpoint %s; use --allow-downgrade to override",
				dump.EntityType,
				dump.DumpDate.Format("2006-01-02"),
				checkpointDate.Time.Format("2006-01-02"),
			)
		}
	}
	return nil
}

func resolveImportContractRevisions(
	entityTypes []string,
) ([]importContractRevision, error) {
	revisions := make([]importContractRevision, len(entityTypes))
	for index, entityType := range entityTypes {
		revision, exists := currentImportContractRevisions[entityType]
		if !exists || revision < 1 {
			return nil, fmt.Errorf(
				"import contract revision is unavailable for entity %s",
				entityType,
			)
		}
		revisions[index] = revision
	}
	return revisions, nil
}

func importContractRevisionSQL(entityTypeColumn string) string {
	return fmt.Sprintf(
		"case %s when '%s' then %d when '%s' then %d when '%s' then %d when '%s' then %d end",
		entityTypeColumn,
		artistEntityType,
		currentImportContractRevisions[artistEntityType],
		labelEntityType,
		currentImportContractRevisions[labelEntityType],
		masterEntityType,
		currentImportContractRevisions[masterEntityType],
		releaseEntityType,
		currentImportContractRevisions[releaseEntityType],
	)
}

func findSuccessfulRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
) (importCheckpointCompatibility, error) {
	var exactRunID sql.NullInt64
	var compatibleEntityCount int
	var selectedEntityCount int
	var incompatibleEntityTypes string
	query := fmt.Sprintf(
		`with candidate_run as (
		    select candidate.id
		      from discogs_import_run candidate
		     where candidate.manifest_sha256 = $1
		       and candidate.status = 'success'
		     order by candidate.completed_at desc, candidate.id desc
		     limit 1
		), expected as (
		    select candidate_dump.entity_type,
		           candidate_dump.dump_id,
		           candidate_dump.import_contract_revision as candidate_revision,
		           %s as expected_revision
		      from candidate_run
		      join discogs_import_run_dump candidate_dump
		        on candidate_dump.import_run_id = candidate_run.id
		), compatibility as (
		    select expected.entity_type,
		           checkpoint.import_run_id,
		           coalesce(
		               current_dump.dump_id = expected.dump_id
		               and current_dump.import_contract_revision = expected.expected_revision
		               and not exists (
		                   select 1
		                     from discogs_import_run_dump failed_dump
		                     join discogs_import_run failed_run
		                       on failed_run.id = failed_dump.import_run_id
		                    where failed_dump.entity_type = expected.entity_type
		                      and failed_run.status = 'failed'
		                      and (failed_run.completed_at > checkpoint.applied_at
		                           or (failed_run.completed_at = checkpoint.applied_at
		                               and failed_run.id > checkpoint.import_run_id))
		               ),
		               false
		           ) as compatible
		      from expected
		      left join discogs_import_checkpoint checkpoint
		        on checkpoint.entity_type = expected.entity_type
		      left join discogs_import_run_dump current_dump
		        on current_dump.import_run_id = checkpoint.import_run_id
		       and current_dump.entity_type = checkpoint.entity_type
		), exact_run as (
		    select candidate_run.id
		      from candidate_run
		     where not exists (
		         select 1
		           from expected
		          where candidate_revision is distinct from expected_revision
		     )
		)
		select (select id from exact_run),
		       count(*) filter (where compatible),
		       count(*),
		       coalesce(
		           string_agg(entity_type, ',' order by entity_type)
		               filter (where not compatible),
		           ''
		       )
		  from compatibility`,
		importContractRevisionSQL("candidate_dump.entity_type"),
	)
	err := tx.QueryRowContext(
		ctx,
		query,
		fingerprint,
	).Scan(
		&exactRunID,
		&compatibleEntityCount,
		&selectedEntityCount,
		&incompatibleEntityTypes,
	)
	if err != nil {
		return importCheckpointCompatibility{}, importContractRevisionQueryError(
			"find successful import manifest",
			err,
		)
	}
	compatibility := importCheckpointCompatibility{
		CompatibleEntityCount: compatibleEntityCount,
		SelectedEntityCount:   selectedEntityCount,
	}
	if exactRunID.Valid {
		compatibility.ExactRunID = exactRunID.Int64
	}
	if incompatibleEntityTypes != "" {
		compatibility.IncompatibleEntityTypes = strings.Split(
			incompatibleEntityTypes,
			",",
		)
	}
	return compatibility, nil
}

func consolidateSuccessfulImportRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
	processorVersion string,
	expectedEntityCount int,
) (int64, error) {
	var runID int64
	if err := tx.QueryRowContext(
		ctx,
		`insert into discogs_import_run
		    (manifest_sha256, status, completed_at, force_requested,
		     allow_downgrade_requested, processor, processor_version)
		 values ($1, 'success', now(), false, false, $2, $3)
		 returning id`,
		fingerprint,
		processorName,
		processorVersion,
	).Scan(&runID); err != nil {
		return 0, fmt.Errorf("record consolidated successful import run: %w", err)
	}
	query := fmt.Sprintf(
		`with candidate_run as (
		    select candidate.id
		      from discogs_import_run candidate
		     where candidate.manifest_sha256 = $2
		       and candidate.status = 'success'
		       and candidate.id <> $1
		     order by candidate.completed_at desc, candidate.id desc
		     limit 1
		), expected as (
		    select candidate_dump.entity_type,
		           candidate_dump.dump_id,
		           %s as import_contract_revision
		      from candidate_run
		      join discogs_import_run_dump candidate_dump
		        on candidate_dump.import_run_id = candidate_run.id
		)
		insert into discogs_import_run_dump
		    (import_run_id, entity_type, dump_id, processed_items,
		     last_progress_at, completed_at, chunk_size, total_items,
		     total_chunks, import_contract_revision)
		select $1, source.entity_type, source.dump_id, source.processed_items,
		       source.last_progress_at, source.completed_at, source.chunk_size,
		       source.total_items, source.total_chunks,
		       expected.import_contract_revision
		  from expected
		  join discogs_import_checkpoint checkpoint
		    on checkpoint.entity_type = expected.entity_type
		  join discogs_import_run_dump source
		    on source.import_run_id = checkpoint.import_run_id
		   and source.entity_type = checkpoint.entity_type
		   and source.dump_id = expected.dump_id
		   and source.import_contract_revision = expected.import_contract_revision`,
		importContractRevisionSQL("candidate_dump.entity_type"),
	)
	result, err := tx.ExecContext(
		ctx,
		query,
		runID,
		fingerprint,
	)
	if err != nil {
		return 0, importContractRevisionQueryError(
			"record consolidated successful import run dumps",
			err,
		)
	}
	recorded, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count consolidated successful import run dumps: %w", err)
	}
	if recorded != int64(expectedEntityCount) {
		return 0, fmt.Errorf(
			"record consolidated successful import run dumps: recorded %d of %d entities",
			recorded,
			expectedEntityCount,
		)
	}
	return runID, nil
}

func findResumableRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
	chunkSize int,
	entityCount int,
) (int64, error) {
	var runID int64
	query := fmt.Sprintf(
		`select import_run.id
		   from discogs_import_run import_run
		  where import_run.manifest_sha256 = $1
		    and import_run.status = 'failed'
		    and not import_run.force_requested
		    and (select count(*)
		           from discogs_import_run_dump run_dump
		          where run_dump.import_run_id = import_run.id) = $2
		    and not exists (
		        select 1
		          from discogs_import_run_dump run_dump
		         where run_dump.import_run_id = import_run.id
		           and run_dump.import_contract_revision is distinct from %s
		    )
		    and not exists (
		        select 1
		          from discogs_import_run_dump run_dump
		         where run_dump.import_run_id = import_run.id
		           and run_dump.chunk_size is distinct from $3
		    )
		    and not exists (
		        select 1
		          from discogs_import_run_dump run_dump
		         where run_dump.import_run_id = import_run.id
		           and run_dump.processed_items <> (
		               select coalesce(sum(run_chunk.item_count), 0)
		                 from discogs_import_run_chunk run_chunk
		                where run_chunk.import_run_id = run_dump.import_run_id
		                  and run_chunk.entity_type = run_dump.entity_type
		           )
		    )
		    and not exists (
		        select 1
		          from discogs_import_run_chunk run_chunk
		          join discogs_import_run_dump run_dump
		            on run_dump.import_run_id = run_chunk.import_run_id
		           and run_dump.entity_type = run_chunk.entity_type
		         where run_chunk.import_run_id = import_run.id
		           and (run_chunk.first_item_index <> run_chunk.chunk_index * run_dump.chunk_size
		                or run_chunk.item_count > run_dump.chunk_size)
		    )
		    and not exists (
		        select 1
		          from discogs_import_run_dump failed_dump
		          join discogs_import_checkpoint checkpoint
		            on checkpoint.entity_type = failed_dump.entity_type
		          join discogs_import_run_dump current_dump
		            on current_dump.import_run_id = checkpoint.import_run_id
		           and current_dump.entity_type = checkpoint.entity_type
		         where failed_dump.import_run_id = import_run.id
		           and (current_dump.dump_id <> failed_dump.dump_id
		                or current_dump.import_contract_revision <>
		                   failed_dump.import_contract_revision)
		           and (checkpoint.applied_at > import_run.completed_at
		                or (checkpoint.applied_at = import_run.completed_at
		                    and checkpoint.import_run_id > import_run.id))
		    )
		  order by import_run.completed_at desc, import_run.id desc
		  limit 1`,
		importContractRevisionSQL("run_dump.entity_type"),
	)
	err := tx.QueryRowContext(
		ctx,
		query,
		fingerprint,
		entityCount,
		chunkSize,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, importContractRevisionQueryError(
			"find resumable import run",
			err,
		)
	}
	return runID, nil
}

func copyResumeProgress(
	ctx context.Context,
	tx *sql.Tx,
	fromRunID int64,
	toRunID int64,
	expectedEntityCount int,
) error {
	summaryResult, err := tx.ExecContext(
		ctx,
		`update discogs_import_run_dump target
		    set processed_items = source.processed_items,
		        total_items = source.total_items,
		        total_chunks = source.total_chunks,
		        last_progress_at = source.last_progress_at,
		        completed_at = source.completed_at
		   from discogs_import_run_dump source
		  where target.import_run_id = $1
		    and source.import_run_id = $2
		    and target.entity_type = source.entity_type
		    and target.dump_id = source.dump_id
		    and target.chunk_size = source.chunk_size`,
		toRunID,
		fromRunID,
	)
	if err != nil {
		return fmt.Errorf("copy import run %d summaries: %w", fromRunID, err)
	}
	copiedSummaries, err := summaryResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("count copied import run %d summaries: %w", fromRunID, err)
	}
	if copiedSummaries != int64(expectedEntityCount) {
		return fmt.Errorf(
			"copy import run %d summaries: copied %d of %d entities",
			fromRunID,
			copiedSummaries,
			expectedEntityCount,
		)
	}

	chunkResult, err := tx.ExecContext(
		ctx,
		`insert into discogs_import_run_chunk
		    (import_run_id, entity_type, chunk_index, first_item_index, item_count, completed_at)
		 select $1, entity_type, chunk_index, first_item_index, item_count, completed_at
		   from discogs_import_run_chunk
		  where import_run_id = $2`,
		toRunID,
		fromRunID,
	)
	if err != nil {
		return fmt.Errorf("copy import run %d chunks: %w", fromRunID, err)
	}
	copiedChunks, err := chunkResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("count copied import run %d chunks: %w", fromRunID, err)
	}
	deleteResult, err := tx.ExecContext(
		ctx,
		"delete from discogs_import_run_chunk where import_run_id = $1",
		fromRunID,
	)
	if err != nil {
		return fmt.Errorf("prune resumed import run %d chunks: %w", fromRunID, err)
	}
	deletedChunks, err := deleteResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("count pruned import run %d chunks: %w", fromRunID, err)
	}
	if deletedChunks != copiedChunks {
		return fmt.Errorf(
			"transfer import run %d chunks: copied %d but pruned %d",
			fromRunID,
			copiedChunks,
			deletedChunks,
		)
	}
	return nil
}

func pruneSupersededFailedProgress(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(
		ctx,
		`delete from discogs_import_run_chunk run_chunk
		  where run_chunk.import_run_id in (
		      select failed_run.id
		        from discogs_import_run failed_run
		       where failed_run.status = 'failed'
		         and not exists (
		             select 1
		               from discogs_import_run_dump failed_dump
		               left join discogs_import_checkpoint checkpoint
		                 on checkpoint.entity_type = failed_dump.entity_type
			               left join discogs_import_run_dump current_dump
		                 on current_dump.import_run_id = checkpoint.import_run_id
		                and current_dump.entity_type = checkpoint.entity_type
			              where failed_dump.import_run_id = failed_run.id
			                and (current_dump.dump_id is distinct from failed_dump.dump_id
			                     or current_dump.import_contract_revision is distinct from
			                        failed_dump.import_contract_revision)
		         )
		  )`,
	); err != nil {
		return fmt.Errorf("prune superseded failed import progress: %w", err)
	}
	return nil
}

func findOrInsertDump(
	ctx context.Context,
	tx *sql.Tx,
	dump *opendiscogsmodel.DiscogsDump,
) (int64, error) {
	var dumpID int64
	err := tx.QueryRowContext(
		ctx,
		`insert into discogs_dump
		    (etag, dump_date, entity_type, checksum_sha256, size_bytes, uri)
		 values ($1, $2, $3, $4, $5, $6)
		 on conflict (dump_date, entity_type, checksum_sha256) do nothing
		 returning id`,
		dump.ETag,
		dump.DumpDate,
		dump.EntityType,
		dump.ChecksumSHA256,
		dump.SizeBytes,
		dump.URI,
	).Scan(&dumpID)
	if err == nil {
		return dumpID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("record %s dump provenance: %w", dump.EntityType, err)
	}
	err = tx.QueryRowContext(
		ctx,
		`select id
		   from discogs_dump
		  where dump_date = $1
		    and entity_type = $2
		    and checksum_sha256 = $3`,
		dump.DumpDate,
		dump.EntityType,
		dump.ChecksumSHA256,
	).Scan(&dumpID)
	if err != nil {
		return 0, fmt.Errorf("resolve %s dump provenance: %w", dump.EntityType, err)
	}
	return dumpID, nil
}

func insertImportRunDump(
	ctx context.Context,
	tx *sql.Tx,
	runID int64,
	entityType string,
	dumpID int64,
	chunkSize int,
	revision importContractRevision,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`insert into discogs_import_run_dump
		    (import_run_id, entity_type, dump_id, chunk_size, import_contract_revision)
		 values ($1, $2, $3, $4, $5)`,
		runID,
		entityType,
		dumpID,
		chunkSize,
		revision,
	); err != nil {
		return importContractRevisionQueryError(
			"record import run dump "+entityType,
			err,
		)
	}
	return nil
}

func insertImportRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
	force bool,
	allowDowngrade bool,
	processorVersion string,
	resumedFromRunID int64,
) (int64, error) {
	var runID int64
	resumedFrom := sql.NullInt64{}
	if resumedFromRunID != 0 {
		resumedFrom = sql.NullInt64{Int64: resumedFromRunID, Valid: true}
	}
	err := tx.QueryRowContext(
		ctx,
		`insert into discogs_import_run
		    (manifest_sha256, status, force_requested,
		     allow_downgrade_requested, processor, processor_version,
		     resumed_from_run_id)
		 values ($1, 'running', $2, $3, $4, $5, $6)
		 returning id`,
		fingerprint,
		force,
		allowDowngrade,
		processorName,
		processorVersion,
		resumedFrom,
	).Scan(&runID)
	if err != nil {
		return 0, importContractRevisionQueryError("start import run", err)
	}
	return runID, nil
}

func importContractRevisionQueryError(operation string, err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == undefinedColumnSQLState {
		return fmt.Errorf(
			"%s: %s is unavailable; apply canonical model migration %s: %w",
			operation,
			importContractRevisionColumnReference,
			importContractRevisionMigration,
			err,
		)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
