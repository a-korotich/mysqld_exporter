package dbacollector

import (
	"context"
	"database/sql"

	cl "github.com/a-korotich/mysqld_exporter/collector"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	rountineMissingQuery = `
		SELECT 
			DISTINCT p.TABLE_SCHEMA
		FROM information_schema.PARTITIONS p
		LEFT JOIN (SELECT
			r.ROUTINE_SCHEMA
			FROM information_schema.ROUTINES r
			WHERE r.ROUTINE_NAME = 'addNewPartition') AS r
			ON r.ROUTINE_SCHEMA = p.TABLE_SCHEMA
		WHERE p.PARTITION_METHOD IN ('RANGE COLUMNS', 'RANGE')
		AND r.ROUTINE_SCHEMA IS NULL
	`
)

var (
	globalRountineMissingDesc = prometheus.NewDesc(
		prometheus.BuildFQName(cl.Namespace, dba, "rountine_missing"),
		"list schemas whitch rountine addNewPartition is missing",
		[]string{"schema"}, nil,
	)
)

type ScrapeRountineMissing struct{}

// Name of the Scraper. Should be unique.
func (ScrapeRountineMissing) Name() string {
	return "rountine_missing"
}

// Help describes the role of the Scraper.
func (ScrapeRountineMissing) Help() string {
	return "Collect schemas whitch rountine addNewPartition is missing"
}

// Version of MySQL from which scraper is available.
func (ScrapeRountineMissing) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeRountineMissing) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	rountineMissingRows, err := db.QueryContext(ctx, rountineMissingQuery)
	if err != nil {
		return err
	}
	defer rountineMissingRows.Close()

	var (
		schema string
		value  float64
	)

	for rountineMissingRows.Next() {
		if err := rountineMissingRows.Scan(
			&schema,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			globalRountineMissingDesc, prometheus.GaugeValue, value,
			schema,
		)
	}
	return nil
}

var _ cl.Scraper = ScrapeRountineMissing{}
