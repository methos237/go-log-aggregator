package executor

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/jamespolk/go-log-aggregator/internal/model"
)

// Labels lists every label name a selector can use: level, the promoted
// stream columns, and each key present in any stream's labels. Sorted.
func Labels(ctx context.Context, db Querier) ([]string, error) {
	rows, err := db.Query(ctx, "SELECT DISTINCT jsonb_object_keys(labels) FROM streams")
	if err != nil {
		return nil, fmt.Errorf("executor: list labels: %w", err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("executor: list labels: %w", err)
	}
	keys = append(keys, "level", "service", "host", "env")
	slices.Sort(keys)
	return keys, nil
}

// promotedValues are the fixed statements for the columns that have their
// own index; the column name is planner-chosen text, never user input.
var promotedValues = map[string]string{
	"service": "SELECT DISTINCT service FROM streams ORDER BY 1",
	"host":    "SELECT DISTINCT host FROM streams ORDER BY 1",
	"env":     "SELECT DISTINCT env FROM streams ORDER BY 1",
}

// LabelValues lists the distinct values of one label across all streams.
// level's values are the level names, since they are fixed by the model.
func LabelValues(ctx context.Context, db Querier, name string) ([]string, error) {
	if name == "level" {
		return model.LevelNames(), nil
	}
	sql, args := promotedValues[name], []any(nil)
	if sql == "" {
		sql, args = "SELECT DISTINCT labels ->> $1 FROM streams WHERE labels ? $1 ORDER BY 1", []any{name}
	}
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("executor: values of %q: %w", name, err)
	}
	values, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("executor: values of %q: %w", name, err)
	}
	return values, nil
}
