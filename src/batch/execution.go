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
)

const processorName = "go-open-discogs-batch"

var fingerprintImportManifest = opendiscogsmanifest.Fingerprint
var orderImportEntityTypes = opendiscogsmanifest.OrderedEntityTypes
var requiredImportLockTypes = opendiscogsmanifest.RequiredLockEntityTypes

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

	successfulRunID, err := findSuccessfulRun(ctx, tx, fingerprint)
	if err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, err
	}
	if successfulRunID != 0 && !force {
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

	dumpIDs := make([]int64, len(dumps))
	for index, dump := range dumps {
		dumpIDs[index], err = findOrInsertDump(ctx, tx, dump)
		if err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, err
		}
	}

	resumedFromRunID := int64(0)
	if !force {
		resumedFromRunID, err = findResumableRun(
			ctx,
			tx,
			fingerprint,
			c.processorVersion,
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
		if _, err := tx.ExecContext(
			ctx,
			`insert into discogs_import_run_dump
			    (import_run_id, entity_type, dump_id, chunk_size)
			 values ($1, $2, $3, $4)`,
			runID,
			dump.EntityType,
			dumpIDs[index],
			chunkSize,
		); err != nil {
			_ = tx.Rollback()
			c.release(ctx)
			return nil, fmt.Errorf("record import run dump %s: %w", dump.EntityType, err)
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
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		c.release(ctx)
		return nil, fmt.Errorf("commit import admission: %w", err)
	}
	committed = true
	c.runID = runID
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
	_ = c.conn.Close()
	c.conn = nil
	c.runID = 0
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

func findSuccessfulRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
) (int64, error) {
	var runID int64
	err := tx.QueryRowContext(
		ctx,
		`select candidate_run.id
		   from discogs_import_run candidate_run
		  where candidate_run.manifest_sha256 = $1
		    and candidate_run.status = 'success'
		    and not exists (
		        select 1
		          from discogs_import_run_dump candidate_dump
		          left join discogs_import_checkpoint checkpoint
		            on checkpoint.entity_type = candidate_dump.entity_type
		          left join discogs_import_run_dump current_dump
		            on current_dump.import_run_id = checkpoint.import_run_id
		           and current_dump.entity_type = checkpoint.entity_type
		         where candidate_dump.import_run_id = candidate_run.id
		           and current_dump.dump_id is distinct from candidate_dump.dump_id
		    )
		    and not exists (
		        select 1
		          from discogs_import_run_dump candidate_dump
		          join discogs_import_checkpoint checkpoint
		            on checkpoint.entity_type = candidate_dump.entity_type
		          join discogs_import_run_dump failed_dump
		            on failed_dump.entity_type = candidate_dump.entity_type
		          join discogs_import_run failed_run
		            on failed_run.id = failed_dump.import_run_id
		         where candidate_dump.import_run_id = candidate_run.id
		           and failed_run.status = 'failed'
		           and (failed_run.completed_at > checkpoint.applied_at
		                or (failed_run.completed_at = checkpoint.applied_at
		                    and failed_run.id > checkpoint.import_run_id))
		    )
		  order by candidate_run.completed_at desc, candidate_run.id desc
		  limit 1`,
		fingerprint,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("find successful import manifest: %w", err)
	}
	return runID, nil
}

func findResumableRun(
	ctx context.Context,
	tx *sql.Tx,
	fingerprint string,
	processorVersion string,
	chunkSize int,
	entityCount int,
) (int64, error) {
	var runID int64
	err := tx.QueryRowContext(
		ctx,
		`select import_run.id
		   from discogs_import_run import_run
		  where import_run.manifest_sha256 = $1
		    and import_run.status = 'failed'
		    and import_run.processor = $2
		    and import_run.processor_version = $3
		    and not import_run.force_requested
		    and (select count(*)
		           from discogs_import_run_dump run_dump
		          where run_dump.import_run_id = import_run.id) = $4
		    and not exists (
		        select 1
		          from discogs_import_run_dump run_dump
		         where run_dump.import_run_id = import_run.id
		           and run_dump.chunk_size is distinct from $5
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
		          join discogs_import_run current_run
		            on current_run.id = checkpoint.import_run_id
		         where failed_dump.import_run_id = import_run.id
		           and (current_dump.dump_id <> failed_dump.dump_id
		                or current_run.processor <> import_run.processor
		                or current_run.processor_version <> import_run.processor_version)
		           and (checkpoint.applied_at > import_run.completed_at
		                or (checkpoint.applied_at = import_run.completed_at
		                    and checkpoint.import_run_id > import_run.id))
		    )
		  order by import_run.completed_at desc, import_run.id desc
		  limit 1`,
		fingerprint,
		processorName,
		processorVersion,
		entityCount,
		chunkSize,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("find resumable import run: %w", err)
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
		               left join discogs_import_run current_run
		                 on current_run.id = checkpoint.import_run_id
		               left join discogs_import_run_dump current_dump
		                 on current_dump.import_run_id = checkpoint.import_run_id
		                and current_dump.entity_type = checkpoint.entity_type
		              where failed_dump.import_run_id = failed_run.id
		                and (current_dump.dump_id is distinct from failed_dump.dump_id
		                     or current_run.processor is distinct from failed_run.processor
		                     or current_run.processor_version is distinct from failed_run.processor_version)
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
		     allow_downgrade_requested, processor, processor_version, resumed_from_run_id)
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
		return 0, fmt.Errorf("start import run: %w", err)
	}
	return runID, nil
}
