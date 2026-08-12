package batch

import (
	"fmt"
	"strings"

	"github.com/dsub-io/go-open-discogs-batch/src/result"
	"github.com/jackc/pgx/v5/pgtype"
)

type integerRelation struct {
	table        string
	parentColumn string
	keyColumn    string
}

type digestIntegerRelation struct {
	table          string
	parentColumn   string
	keyColumn      string
	identityColumn string
}

type textRelation struct {
	table        string
	parentColumn string
	keyColumn    string
}

type twoIntegerKeyRelation struct {
	table           string
	parentColumn    string
	firstKeyColumn  string
	secondKeyColumn string
}

type digestTwoIntegerKeyRelation struct {
	table           string
	parentColumn    string
	firstKeyColumn  string
	secondKeyColumn string
	identityColumn  string
}

type integerNullableTextKeyRelation struct {
	table             string
	parentColumn      string
	integerKeyColumn  string
	nullableKeyColumn string
}

func reconcileIntegerRelation[T any](
	order Order,
	relation integerRelation,
	deleteStale bool,
	rootIDs []int32,
	incoming []T,
	parentID func(T) int32,
	key func(T) int32,
) result.Result {
	if !deleteStale {
		return doWrite(incoming, order.getChunkSize(), order.getDB())
	}
	parents := make([]int32, 0, len(incoming))
	keys := make([]int32, 0, len(incoming))
	for _, item := range incoming {
		parents = append(parents, parentID(item))
		keys = append(keys, key(item))
	}
	deleted := order.getDB().Exec(
		fmt.Sprintf(
			`delete from %s current
			  where current.%s = any(?::integer[])
			    and not exists (
			        select 1
			          from unnest(?::integer[], ?::integer[]) incoming(parent_id, relation_key)
			         where incoming.parent_id = current.%s
			           and incoming.relation_key = current.%s
			    )`,
			relation.table,
			relation.parentColumn,
			relation.parentColumn,
			relation.keyColumn,
		),
		postgresArray(rootIDs),
		postgresArray(parents),
		postgresArray(keys),
	)
	if deleted.Error != nil {
		return result.NewResult(0, fmt.Errorf("delete stale %s rows: %w", relation.table, deleted.Error))
	}
	written := doWrite(incoming, order.getChunkSize(), order.getDB())
	return result.NewResult(int(deleted.RowsAffected), nil).Sum(written)
}

func reconcileDigestIntegerRelation[T any](
	order Order,
	relation digestIntegerRelation,
	deleteStale bool,
	rootIDs []int32,
	incoming []T,
	parentID func(T) int32,
	key func(T) int32,
	identity func(T) []byte,
) result.Result {
	if !deleteStale {
		return doWrite(incoming, order.getChunkSize(), order.getDB())
	}
	parents := make([]int32, 0, len(incoming))
	keys := make([]int32, 0, len(incoming))
	identities := make([][]byte, 0, len(incoming))
	for _, item := range incoming {
		parents = append(parents, parentID(item))
		keys = append(keys, key(item))
		identities = append(identities, identity(item))
	}
	deleted := order.getDB().Exec(
		fmt.Sprintf(
			`delete from %s current
			  where current.%s = any(?::integer[])
			    and not exists (
			        select 1
			          from unnest(?::integer[], ?::integer[], ?::bytea[])
			               incoming(parent_id, relation_key, identity_sha256)
			         where incoming.parent_id = current.%s
			           and incoming.relation_key = current.%s
			           and incoming.identity_sha256 = current.%s
			    )`,
			relation.table,
			relation.parentColumn,
			relation.parentColumn,
			relation.keyColumn,
			relation.identityColumn,
		),
		postgresArray(rootIDs),
		postgresArray(parents),
		postgresArray(keys),
		postgresArray(identities),
	)
	if deleted.Error != nil {
		return result.NewResult(0, fmt.Errorf("delete stale %s rows: %w", relation.table, deleted.Error))
	}
	written := doWrite(incoming, order.getChunkSize(), order.getDB())
	return result.NewResult(int(deleted.RowsAffected), nil).Sum(written)
}

func reconcileTextRelation[T any](
	order Order,
	relation textRelation,
	deleteStale bool,
	rootIDs []int32,
	incoming []T,
	parentID func(T) int32,
	key func(T) string,
) result.Result {
	if !deleteStale {
		return doWrite(incoming, order.getChunkSize(), order.getDB())
	}
	parents := make([]int32, 0, len(incoming))
	keys := make([]string, 0, len(incoming))
	for _, item := range incoming {
		parents = append(parents, parentID(item))
		keys = append(keys, key(item))
	}
	deleted := order.getDB().Exec(
		fmt.Sprintf(
			`delete from %s current
			  where current.%s = any(?::integer[])
			    and not exists (
			        select 1
			          from unnest(?::integer[], ?::text[]) incoming(parent_id, relation_key)
			         where incoming.parent_id = current.%s
			           and incoming.relation_key = current.%s
			    )`,
			relation.table,
			relation.parentColumn,
			relation.parentColumn,
			relation.keyColumn,
		),
		postgresArray(rootIDs),
		postgresArray(parents),
		postgresArray(keys),
	)
	if deleted.Error != nil {
		return result.NewResult(0, fmt.Errorf("delete stale %s rows: %w", relation.table, deleted.Error))
	}
	written := doWrite(incoming, order.getChunkSize(), order.getDB())
	return result.NewResult(int(deleted.RowsAffected), nil).Sum(written)
}

func reconcileTwoIntegerKeyRelation[T any](
	order Order,
	relation twoIntegerKeyRelation,
	deleteStale bool,
	rootIDs []int32,
	incoming []T,
	parentID func(T) int32,
	firstKey func(T) int32,
	secondKey func(T) int32,
) result.Result {
	if !deleteStale {
		return doWrite(incoming, order.getChunkSize(), order.getDB())
	}
	parents := make([]int32, 0, len(incoming))
	firstKeys := make([]int32, 0, len(incoming))
	secondKeys := make([]int32, 0, len(incoming))
	for _, item := range incoming {
		parents = append(parents, parentID(item))
		firstKeys = append(firstKeys, firstKey(item))
		secondKeys = append(secondKeys, secondKey(item))
	}
	deleted := order.getDB().Exec(
		fmt.Sprintf(
			`delete from %s current
			  where current.%s = any(?::integer[])
			    and not exists (
			        select 1
			          from unnest(?::integer[], ?::integer[], ?::integer[])
			               incoming(parent_id, first_key, second_key)
			         where incoming.parent_id = current.%s
			           and incoming.first_key = current.%s
			           and incoming.second_key = current.%s
			    )`,
			relation.table,
			relation.parentColumn,
			relation.parentColumn,
			relation.firstKeyColumn,
			relation.secondKeyColumn,
		),
		postgresArray(rootIDs),
		postgresArray(parents),
		postgresArray(firstKeys),
		postgresArray(secondKeys),
	)
	if deleted.Error != nil {
		return result.NewResult(0, fmt.Errorf("delete stale %s rows: %w", relation.table, deleted.Error))
	}
	written := doWrite(incoming, order.getChunkSize(), order.getDB())
	return result.NewResult(int(deleted.RowsAffected), nil).Sum(written)
}

func reconcileDigestTwoIntegerKeyRelation[T any](
	order Order,
	relation digestTwoIntegerKeyRelation,
	deleteStale bool,
	rootIDs []int32,
	incoming []T,
	parentID func(T) int32,
	firstKey func(T) int32,
	secondKey func(T) int32,
	identity func(T) []byte,
) result.Result {
	if !deleteStale {
		return doWrite(incoming, order.getChunkSize(), order.getDB())
	}
	parents := make([]int32, 0, len(incoming))
	firstKeys := make([]int32, 0, len(incoming))
	secondKeys := make([]int32, 0, len(incoming))
	identities := make([][]byte, 0, len(incoming))
	for _, item := range incoming {
		parents = append(parents, parentID(item))
		firstKeys = append(firstKeys, firstKey(item))
		secondKeys = append(secondKeys, secondKey(item))
		identities = append(identities, identity(item))
	}
	deleted := order.getDB().Exec(
		fmt.Sprintf(
			`delete from %s current
			  where current.%s = any(?::integer[])
			    and not exists (
			        select 1
			          from unnest(?::integer[], ?::integer[], ?::integer[], ?::bytea[])
			               incoming(parent_id, first_key, second_key, identity_sha256)
			         where incoming.parent_id = current.%s
			           and incoming.first_key = current.%s
			           and incoming.second_key = current.%s
			           and incoming.identity_sha256 = current.%s
			    )`,
			relation.table,
			relation.parentColumn,
			relation.parentColumn,
			relation.firstKeyColumn,
			relation.secondKeyColumn,
			relation.identityColumn,
		),
		postgresArray(rootIDs),
		postgresArray(parents),
		postgresArray(firstKeys),
		postgresArray(secondKeys),
		postgresArray(identities),
	)
	if deleted.Error != nil {
		return result.NewResult(0, fmt.Errorf("delete stale %s rows: %w", relation.table, deleted.Error))
	}
	written := doWrite(incoming, order.getChunkSize(), order.getDB())
	return result.NewResult(int(deleted.RowsAffected), nil).Sum(written)
}

func reconcileIntegerNullableTextKeyRelation[T any](
	order Order,
	relation integerNullableTextKeyRelation,
	deleteStale bool,
	rootIDs []int32,
	incoming []T,
	parentID func(T) int32,
	integerKey func(T) int32,
	nullableTextKey func(T) *string,
) result.Result {
	if !deleteStale {
		return doWrite(incoming, order.getChunkSize(), order.getDB())
	}
	parents := make([]int32, 0, len(incoming))
	integerKeys := make([]int32, 0, len(incoming))
	textKeys := make([]string, 0, len(incoming))
	hasTextKeys := make([]bool, 0, len(incoming))
	for _, item := range incoming {
		parents = append(parents, parentID(item))
		integerKeys = append(integerKeys, integerKey(item))
		textKey := nullableTextKey(item)
		hasTextKeys = append(hasTextKeys, textKey != nil)
		if textKey == nil {
			textKeys = append(textKeys, "")
		} else {
			textKeys = append(textKeys, *textKey)
		}
	}
	deleted := order.getDB().Exec(
		fmt.Sprintf(
			`delete from %s current
			  where current.%s = any(?::integer[])
			    and not exists (
			        select 1
			          from unnest(?::integer[], ?::integer[], ?::text[], ?::boolean[])
			               incoming(parent_id, integer_key, text_key, has_text_key)
			         where incoming.parent_id = current.%s
			           and incoming.integer_key = current.%s
			           and incoming.has_text_key = (current.%s is not null)
			           and (not incoming.has_text_key or incoming.text_key = current.%s)
			    )`,
			relation.table,
			relation.parentColumn,
			relation.parentColumn,
			relation.integerKeyColumn,
			relation.nullableKeyColumn,
			relation.nullableKeyColumn,
		),
		postgresArray(rootIDs),
		postgresArray(parents),
		postgresArray(integerKeys),
		postgresArray(textKeys),
		postgresArray(hasTextKeys),
	)
	if deleted.Error != nil {
		return result.NewResult(0, fmt.Errorf("delete stale %s rows: %w", relation.table, deleted.Error))
	}
	written := doWrite(incoming, order.getChunkSize(), order.getDB())
	return result.NewResult(int(deleted.RowsAffected), nil).Sum(written)
}

func postgresArray[T any](values []T) pgtype.Array[T] {
	return pgtype.Array[T]{
		Elements: values,
		Dims: []pgtype.ArrayDimension{
			{Length: int32(len(values)), LowerBound: 1},
		},
		Valid: true,
	}
}

func relationTablesContainRows(order Order, tables ...string) (bool, error) {
	checks := make([]string, len(tables))
	for index, table := range tables {
		checks[index] = fmt.Sprintf("exists (select 1 from %s limit 1)", table)
	}
	var containsRows bool
	query := order.getDB().Raw(
		"select " + strings.Join(checks, " or "),
	).Scan(&containsRows)
	if query.Error != nil {
		return false, fmt.Errorf("check relation table state: %w", query.Error)
	}
	return containsRows, nil
}
