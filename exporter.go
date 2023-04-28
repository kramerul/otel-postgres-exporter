package hanaexporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"database/sql"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"

	// Register hdb driver.
	hana "github.com/SAP/go-hdb/driver"
)

type tracesExporter struct {
	db      *sql.DB
	started bool
}

const (
	driverName = "hdb"
)

func newTracesExporter(config *Config, settings exporter.CreateSettings) (*tracesExporter, error) {

	var db, err = sql.Open(driverName, config.Dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	_, err = db.Exec(`
         CREATE TABLE SQL_TRACES ( 
			STATEMENT VARCHAR(5000) NOT NULL, 
			START_TIME TIMESTAMP NOT NULL, 
			END_TIME TIMESTAMP NOT NULL, 
			ATTRIBUTES VARCHAR(5000) NOT NULL,
			PRIMARY KEY(START_TIME)
			)`)
	if err != nil {
		var dbError hana.Error
		if errors.As(err, &dbError) {
			if dbError.Code() != 288 {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return &tracesExporter{
		db:      db,
		started: false,
	}, nil
}

func (e *tracesExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		var span = td.ResourceSpans().At(i).ScopeSpans()
		for k := 0; k < span.Len(); k++ {
			var s = span.At(k)
			for j := 0; j < s.Spans().Len(); j++ {
				var x = s.Spans().At(j)
				var attributes = x.Attributes().AsRaw()
				if _, ok := attributes["sql"]; ok {
					fmt.Printf("Span %v\n", attributes)
					jsonStr, err := json.Marshal(attributes)
					if err != nil {
						return err
					}
					_, err = e.db.Exec(" INSERT INTO SQL_TRACES (STATEMENT,START_TIME,END_TIME,ATTRIBUTES) VALUES(?,?,?,?)",
						attributes["sql"], x.StartTimestamp().AsTime(), x.EndTimestamp().AsTime(), jsonStr)
					if err != nil {
						return err
					}
				}
			}
		}

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
