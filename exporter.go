package hanaexporter

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type tracesExporter struct {
	started bool
}

func newTracesExporter(config *Config, settings exporter.CreateSettings) (*tracesExporter, error) {

	var _, err = pgx.Connect(context.Background(), config.Url)
	if err != nil {
		return nil, err
	}

	return &tracesExporter{
		started: false,
	}, nil
}

func (e *tracesExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
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
