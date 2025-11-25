package dbacollector

import (
	"context"
	"database/sql"

	cl "github.com/a-korotich/mysqld_exporter/collector"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	disabledEventsOnActiveNodeQuery = `
		SELECT
		e.EVENT_SCHEMA AS schema,
		e.EVENT_NAME AS event
		FROM information_schema.EVENTS e
		WHERE TRUE
		AND @@read_only <=> 0
		AND e.STATUS <> 'ENABLED'
	`
)

var (
	disabledEventsOnActiveNodeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(cl.Namespace, dba, "disabled_events_on_active_node"),
		"List schemas and events whose status is not ENABLED on the active (master) node.",
		[]string{"schema", "event"}, nil,
	)
)

type ScrapeDisabledEventsOnActiveNode struct{}

// Name of the Scraper. Should be unique.
func (ScrapeDisabledEventsOnActiveNode) Name() string {
	return "disabled_events_on_active_node"
}

// Help describes the role of the Scraper.
func (ScrapeDisabledEventsOnActiveNode) Help() string {
	return "Collect schemas whitch rountine addNewPartition is missing"
}

// Version of MySQL from which scraper is available.
func (ScrapeDisabledEventsOnActiveNode) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeDisabledEventsOnActiveNode) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	disabledEventsOnActiveNodeRows, err := db.QueryContext(ctx, disabledEventsOnActiveNodeQuery)
	if err != nil {
		return err
	}
	defer disabledEventsOnActiveNodeRows.Close()

	var (
		schema, event string
		value         float64
	)

	for disabledEventsOnActiveNodeRows.Next() {
		if err := disabledEventsOnActiveNodeRows.Scan(
			&schema, &event,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			disabledEventsOnActiveNodeDesc, prometheus.GaugeValue, value,
			schema,
		)
	}
	return nil
}

var _ cl.Scraper = ScrapeDisabledEventsOnActiveNode{}
