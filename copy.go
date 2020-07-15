package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

type copy struct {
	FileName   string  `db:"FILE_NAME"`
	RowCount   float64 `db:"ROW_COUNT"`
	RowParsed  float64 `db:"ROW_PARSED"`
	FileSize   float64 `db:"FILE_SIZE"`
	ErrorCount float64 `db:"ERROR_COUNT"`
	Status     string  `db:"STATUS"`
	Table      string  `db:"TABLE_NAME"`
	Schema     string  `db:"TABLE_SCHEMA_NAME"`
	Database   string  `db:"TABLE_CATALOG_NAME"`
}

var (
	copyLabels        = []string{"table", "schema", "database", "status"}
	rowsLoadedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "rows_loaded_count",
		Subsystem:   "copy",
		Namespace:   "snowflake",
		Help:        "Number of rows loaded from the source file.",
		ConstLabels: prometheus.Labels{"account": account},
	}, copyLabels)

	errorRowCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "error_count",
		Subsystem:   "copy",
		Namespace:   "snowflake",
		Help:        "Number of error rows in the source file.",
		ConstLabels: prometheus.Labels{"account": account},
	}, copyLabels)

	parsedRowCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "parse_count",
		Subsystem:   "copy",
		Namespace:   "snowflake",
		Help:        "Number of rows parsed from the source file;``NULL`` if STATUS is ‘LOAD_IN_PROGRESS’.",
		ConstLabels: prometheus.Labels{"account": account},
	}, copyLabels)

	copyCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "count",
		Subsystem:   "copy",
		Namespace:   "snowflake",
		ConstLabels: prometheus.Labels{"account": account},
	}, copyLabels)

	successGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "success",
		Subsystem:   "copy",
		Namespace:   "snowflake",
		ConstLabels: prometheus.Labels{"account": account},
	}, copyLabels)
)

func gatherCopyMetrics(table string, db *sql.DB, start chan time.Time, done chan bool) {
	for rangeStart := range start {
		if !dry {
			query := fmt.Sprintf("select * from table(information_schema.copy_history(TABLE_NAME => '%s', START_TIME=> to_timestamp_ltz('%s'), END_TIME => current_timestamp()));", table, rangeStart.Format(time.RFC3339))
			log.Debugf("[CopyMetrics] Query: %s", query)
			rows, err := runQuery(query, db)
			if err != nil {
				log.Errorf("Failed to query db for copy history. %+v", err)
				done <- true
				continue
			}

			done <- true

			copy := &copy{}
			for rows.Next() {
				rows.StructScan(copy)
				log.Debugf("[CopyMetrics] Row: %+v", copy)

				copyCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Inc()
				if copy.Status == "LOADED" {
					successGauge.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Set(1)
				}

				rowsLoadedCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Add(copy.RowCount)
				errorRowCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Add(copy.ErrorCount)
				parsedRowCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Add(copy.RowParsed)
			}

			rows.Close()
		} else {
			log.Info("[CopyMetrics] Skipping query execution due to presence of dry-run flag")
			done <- true
		}
	}
}
