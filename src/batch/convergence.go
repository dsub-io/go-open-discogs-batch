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

func reconcileIntegerRelation[T comparable](
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
			`delete from public.%s current
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

func reconcileTextRelation[T comparable](
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
			`delete from public.%s current
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

func reconcileTwoIntegerKeyRelation[T comparable](
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
			`delete from public.%s current
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

func postgresArray[T comparable](values []T) pgtype.Array[T] {
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
		checks[index] = fmt.Sprintf("exists (select 1 from public.%s limit 1)", table)
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
