package dbacollector

import (
	"context"
	"database/sql"

	cl "github.com/a-korotich/mysqld_exporter/collector"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	OpenTablesQuery = `
		SHOW OPEN TABLES WHERE In_use > 0
	`
)

var (
	globalOpenTablesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(cl.Namespace, dba, "open_tables"),
		"lists tables that are currently open in the table cache",
		[]string{"schema", "table"}, nil,
	)
)

type ScrapeOpenTables struct{}

// Name of the Scraper. Should be unique.
func (ScrapeOpenTables) Name() string {
	return "open_tables"
}

// Help describes the role of the Scraper.
func (ScrapeOpenTables) Help() string {
	return "Collect tables that are currently open"
}

// Version of MySQL from which scraper is available.
func (ScrapeOpenTables) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeOpenTables) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	openTablesRows, err := db.QueryContext(ctx, OpenTablesQuery)
	if err != nil {
		return err
	}
	defer openTablesRows.Close()

	var (
		schema, table string
		value         float64
	)

	for openTablesRows.Next() {
		if err := openTablesRows.Scan(
			&schema, &table, &value,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			globalOpenTablesDesc, prometheus.GaugeValue, value,
			schema, table,
		)
	}
	return nil
}

var _ cl.Scraper = ScrapeOpenTables{}
