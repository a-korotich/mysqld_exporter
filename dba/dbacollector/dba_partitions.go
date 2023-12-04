package dbacollector

import (
	"context"
	"database/sql"

	cl "github.com/a-korotich/mysqld_exporter/collector"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	UsingPartitionsQuery = `
	SELECT
	a.TABLE_SCHEMA,
	a.TABLE_NAME,
	a.PARTITION_NAME,
	CAST(a.PARTITION_DESCRIPTION AS UNSIGNED)
  FROM information_schema.PARTITIONS AS a
	INNER JOIN (SELECT
		TABLE_CATALOG,
		TABLE_SCHEMA,
		TABLE_NAME,
		MAX(PARTITION_ORDINAL_POSITION) POSITION
	  FROM information_schema.PARTITIONS
	  WHERE PARTITION_METHOD = 'RANGE'
	  AND PARTITION_DESCRIPTION != 'MAXVALUE'
	  GROUP BY 1,
			   2,
			   3) AS b
	  ON a.TABLE_CATALOG = b.TABLE_CATALOG
	  AND a.TABLE_SCHEMA = b.TABLE_SCHEMA
	  AND a.TABLE_NAME = b.TABLE_NAME
	  AND a.PARTITION_ORDINAL_POSITION = b.POSITION
  WHERE CAST(PARTITION_DESCRIPTION AS UNSIGNED) < UNIX_TIMESTAMP(DATE_FORMAT(NOW() + INTERVAL 2 MONTH, '%Y-%m-01 00:00:00'))
  UNION ALL
  SELECT
	a.TABLE_SCHEMA,
	a.TABLE_NAME,
	a.PARTITION_NAME,
	UNIX_TIMESTAMP(STR_TO_DATE(REPLACE(PARTITION_DESCRIPTION, "'", ''), '%Y-%m-%d'))
  FROM information_schema.PARTITIONS AS a
	INNER JOIN (SELECT
		TABLE_CATALOG,
		TABLE_SCHEMA,
		TABLE_NAME,
		MAX(PARTITION_ORDINAL_POSITION) POSITION
	  FROM information_schema.PARTITIONS
	  WHERE PARTITION_METHOD = 'RANGE COLUMNS'
	  AND PARTITION_DESCRIPTION != 'MAXVALUE'
	  GROUP BY 1,
			   2,
			   3) AS b
	  ON a.TABLE_CATALOG = b.TABLE_CATALOG
	  AND a.TABLE_SCHEMA = b.TABLE_SCHEMA
	  AND a.TABLE_NAME = b.TABLE_NAME
	  AND a.PARTITION_ORDINAL_POSITION = b.POSITION
	  AND STR_TO_DATE(REPLACE(PARTITION_DESCRIPTION, "'", ''), '%Y-%m-%d') < DATE(DATE_FORMAT(NOW() + INTERVAL 2 MONTH, '%Y-%m-01 00:00:00'))
  
`
)

var (
	globalUsingPartitionsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(cl.Namespace, dba, "using_partitions"),
		"Listing tables where partitions are used.",
		[]string{"schema", "table", "partition"}, nil,
	)
)

type ScrapeUsingPartitions struct{}

// Name of the Scraper. Should be unique.
func (ScrapeUsingPartitions) Name() string {
	return "using_partitions"
}

// Help describes the role of the Scraper.
func (ScrapeUsingPartitions) Help() string {
	return "Collect tables where partitions are used"
}

// Version of MySQL from which scraper is available.
func (ScrapeUsingPartitions) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeUsingPartitions) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	usingPartitionsRows, err := db.QueryContext(ctx, UsingPartitionsQuery)
	if err != nil {
		return err
	}
	defer usingPartitionsRows.Close()

	var (
		schema, table, partition string
		value                    float64
	)

	for usingPartitionsRows.Next() {
		if err := usingPartitionsRows.Scan(
			&schema, &table, &partition, &value,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			globalUsingPartitionsDesc, prometheus.GaugeValue, value,
			schema, table, partition,
		)
	}
	return nil
}

var _ cl.Scraper = ScrapeUsingPartitions{}
