package database

import (
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
)

const (
	DefaultSchemaName       = "public"
	canonicalSchemaPrefix   = "public."
	maximumSchemaNameLength = 63
	searchPathParameter     = "search_path"
)

var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Schema is a validated PostgreSQL schema selected by an operator.
type Schema struct {
	name       string
	identifier string
}

type databaseCreatePrivilege struct {
	Name    string `gorm:"column:name"`
	Allowed bool   `gorm:"column:allowed"`
}

// ParseSchema validates a portable, unquoted PostgreSQL schema name.
func ParseSchema(name string) (Schema, error) {
	trimmed := strings.TrimSpace(name)
	if len(trimmed) == 0 || len(trimmed) > maximumSchemaNameLength || !schemaNamePattern.MatchString(trimmed) {
		return Schema{}, fmt.Errorf(
			"database-schema must be 1 to %d lowercase letters, digits, or underscores and start with a letter or underscore",
			maximumSchemaNameLength,
		)
	}
	return Schema{name: trimmed, identifier: `"` + trimmed + `"`}, nil
}

// ValidateSchemaName validates the public CLI and ENV contract.
func ValidateSchemaName(name string) error {
	_, err := ParseSchema(name)
	return err
}

func (schema Schema) Name() string {
	return schema.name
}

func (schema Schema) Identifier() string {
	return schema.identifier
}

// SearchPath keeps public available for database-wide extension objects while selecting the
// requested schema first for every unqualified canonical query.
func (schema Schema) SearchPath() string {
	if schema.name == DefaultSchemaName {
		return schema.identifier
	}
	return schema.identifier + `, "` + DefaultSchemaName + `"`
}

func (schema Schema) Qualify(table string) string {
	return schema.identifier + `."` + table + `"`
}

// ScopeCanonicalSQL maps immutable model migrations from their canonical public schema to the
// operator-selected schema without changing the migration bytes used for checksum verification.
func (schema Schema) ScopeCanonicalSQL(statement string) string {
	return strings.ReplaceAll(statement, canonicalSchemaPrefix, schema.identifier+".")
}

// EnsureSchema creates a missing schema and verifies the DDL privileges required by migrations.
func EnsureSchema(db *gorm.DB, schema Schema) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("prepare database schema %s: database connection is nil", schema.name)
	}
	var exists bool
	if err := db.Raw(
		"select exists(select 1 from pg_namespace where nspname = ?)",
		schema.name,
	).Scan(&exists).Error; err != nil {
		return false, fmt.Errorf("inspect database schema %s: %w", schema.name, err)
	}
	created := false
	if !exists {
		var privilege databaseCreatePrivilege
		if err := db.Raw(
			"select current_database() as name, has_database_privilege(current_user, current_database(), 'CREATE') as allowed",
		).Scan(&privilege).Error; err != nil {
			return false, fmt.Errorf("inspect CREATE privilege for database schema %s: %w", schema.name, err)
		}
		if !privilege.Allowed {
			return false, fmt.Errorf(
				"database schema %s does not exist and current user lacks CREATE on database %s; pre-create the schema or grant database CREATE",
				schema.name,
				privilege.Name,
			)
		}
		if err := db.Exec(
			"create schema if not exists " + schema.identifier + " authorization current_user",
		).Error; err != nil {
			return false, fmt.Errorf("create database schema %s: %w", schema.name, err)
		}
		created = true
	}
	var canUseAndCreate bool
	if err := db.Raw(
		"select has_schema_privilege(current_user, ?, 'USAGE') and has_schema_privilege(current_user, ?, 'CREATE')",
		schema.name,
		schema.name,
	).Scan(&canUseAndCreate).Error; err != nil {
		return false, fmt.Errorf("inspect privileges for database schema %s: %w", schema.name, err)
	}
	if !canUseAndCreate {
		return false, fmt.Errorf(
			"current user requires USAGE and CREATE privileges on database schema %s; grant both privileges or use a writable schema",
			schema.name,
		)
	}
	return created, nil
}
