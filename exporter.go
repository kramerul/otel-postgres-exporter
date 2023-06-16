package pgexporter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type tracesExporter struct {
	db      *pgx.Conn
	started bool
}

const (
	driverName = "pgx"
)

func newTracesExporter(config *Config, settings exporter.CreateSettings) (*tracesExporter, error) {

	var db, err = pgx.Connect(context.Background(), config.Dsn)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(context.TODO(), `
         CREATE TABLE IF NOT EXISTS  traces ( 
			trace_id VARCHAR(32) NOT NULL,
			parent_id VARCHAR(16),
			id VARCHAR(100),
			name VARCHAR(100),
			kind VARCHAR(100),
			range tsrange NOT NULL,
			attributes JSONB,
			PRIMARY KEY(id)
			)`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(context.TODO(), `
	CREATE INDEX IF NOT EXISTS traces_range_idx ON traces (range);`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(context.TODO(), `
	CREATE INDEX IF NOT EXISTS traces_attributes_idx ON traces USING GIN (attributes);`)
	if err != nil {
		return nil, err
	}

	return &tracesExporter{
		db:      db,
		started: false,
	}, nil
}

func (e *tracesExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	rows := make([][]interface{}, 0)
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		var span = td.ResourceSpans().At(i).ScopeSpans()
		for k := 0; k < span.Len(); k++ {
			var s = span.At(k)
			for j := 0; j < s.Spans().Len(); j++ {
				var x = s.Spans().At(j)
				var attributes = x.Attributes().AsRaw()
				fmt.Printf("Span %v\n", attributes)
				jsonStr, err := json.Marshal(attributes)
				if err != nil {
					return err
				}
				spanRange := pgtype.Range[pgtype.Timestamp]{
					Lower:     pgtype.Timestamp{Time: x.StartTimestamp().AsTime(), Valid: true, InfinityModifier: pgtype.Finite},
					Upper:     pgtype.Timestamp{Time: x.EndTimestamp().AsTime(), Valid: true, InfinityModifier: pgtype.Finite},
					LowerType: pgtype.Inclusive,
					UpperType: pgtype.Exclusive,
					Valid:     true,
				}
				rows = append(rows, []interface{}{x.TraceID().String(), x.ParentSpanID().String(), x.SpanID().String(), x.Name(), x.Kind(), spanRange, jsonStr})
			}
		}

	}
	_, err := e.db.CopyFrom(context.TODO(),
		pgx.Identifier{"traces"},
		[]string{"trace_id", "parent_id", "id", "name", "kind", "range", "attributes"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return err
	}
	return nil
}

func (e *tracesExporter) Start(ctx context.Context, host component.Host) error {
	e.started = true
	return nil
}

func (e *tracesExporter) Shutdown(ctx context.Context) error {
	if !e.started {
		return nil
	}
	e.started = false
	return nil
}
