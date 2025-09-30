package dbacollector

import (
	"context"
	"database/sql"

	cl "github.com/a-korotich/mysqld_exporter/collector"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	PartitionsToRemoveQuery = `
SELECT
	a.TABLE_SCHEMA,
	a.TABLE_NAME,
	a.PARTITION_NAME,
	ROUND((a.DATA_LENGTH + a.INDEX_LENGTH) / 1024 / 1024, 2) AS PARTITION_SIZE_MB,
	CAST(a.PARTITION_DESCRIPTION AS UNSIGNED) AS PARTITION_TIMESTAMP
FROM information_schema.PARTITIONS AS a
WHERE STR_TO_DATE(a.PARTITION_DESCRIPTION, "'%Y-%m-%d'") IS NULL
  AND CAST(a.PARTITION_DESCRIPTION AS UNSIGNED) <= UNIX_TIMESTAMP(DATE_FORMAT(NOW() - INTERVAL 6 MONTH, '%Y-%m-01 00:00:00'))

UNION ALL

SELECT
	a.TABLE_SCHEMA,
	a.TABLE_NAME,
	a.PARTITION_NAME,
    ROUND((a.DATA_LENGTH + a.INDEX_LENGTH) / 1024 / 1024, 2) AS PARTITION_SIZE_MB,
	UNIX_TIMESTAMP(STR_TO_DATE(REPLACE(a.PARTITION_DESCRIPTION, "'", ''), '%Y-%m-%d')) AS PARTITION_TIMESTAMP

FROM information_schema.PARTITIONS AS a
WHERE STR_TO_DATE(a.PARTITION_DESCRIPTION, "'%Y-%m-%d'") <= DATE(DATE_FORMAT(NOW() - INTERVAL 6 MONTH, '%Y-%m-01 00:00:00'));
`
)

var (
	globalPartitionsToRemoveDesc = prometheus.NewDesc(
		prometheus.BuildFQName(cl.Namespace, dba, "partitions_to_remove"),
		"Collect tables where partitions older than 6 month",
		[]string{"schema", "table", "partition","partitionSize"}, nil,
	)
)

type ScrapePartitionsToRemove struct{}

// Name of the Scraper. Should be unique.
func (ScrapePartitionsToRemove) Name() string {
	return "partitions_to_remove"
}

// Help describes the role of the Scraper.
func (ScrapePartitionsToRemove) Help() string {
	return "Collect tables where partitions older than 6 month"
}

// Version of MySQL from which scraper is available.
func (ScrapePartitionsToRemove) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePartitionsToRemove) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	partitionsToRemoveRows, err := db.QueryContext(ctx, PartitionsToRemoveQuery)
	if err != nil {
		return err
	}
	defer partitionsToRemoveRows.Close()

	var (
		schema, table, partition, partitionSize string
		value                    float64
	)

	for partitionsToRemoveRows.Next() {
		if err := partitionsToRemoveRows.Scan(
			&schema, &table, &partition, &partitionSize, &value,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			globalPartitionsToRemoveDesc, prometheus.GaugeValue, value,
			schema, table, partition, partitionSize,
		)
	}
	return nil
}

var _ cl.Scraper = ScrapePartitionsToRemove{}
