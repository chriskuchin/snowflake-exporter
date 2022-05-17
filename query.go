package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

type queryRow struct {
	ID                     string  `db:"QUERY_ID"`
	Text                   string  `db:"QUERY_TEXT"`
	Status                 string  `db:"EXECUTION_STATUS"`
	User                   string  `db:"USER_NAME"`
	Warehouse              string  `db:"WAREHOUSE_NAME"`
	Schema                 string  `db:"SCHEMA_NAME"`
	Database               string  `db:"DATABASE_NAME"`
	ErrorCode              *string `db:"ERROR_CODE"`
	ErrorMessage           *string `db:"ERROR_MESSAGE"`
	ElapsedTime            float64 `db:"TOTAL_ELAPSED_TIME"`
	BytesScanned           float64 `db:"BYTES_SCANNED"`
	RowsProduced           float64 `db:"ROWS_PRODUCED"`
	CompilationTime        float64 `db:"COMPILATION_TIME"`
	Executiontime          float64 `db:"EXECUTION_TIME"`
	QueuedProvisioningTime float64 `db:"QUEUED_PROVISIONING_TIME"`
	QueuedRepairTime       float64 `db:"QUEUED_REPAIR_TIME"`
	QueuedOverloadTime     float64 `db:"QUEUED_OVERLOAD_TIME"`
	TransactionBlockedTime float64 `db:"TRANSACTION_BLOCKED_TIME"`
}

var (
	queryLabels = []string{"user", "warehouse", "schema", "database", "status"}

	bytesScannedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "bytes_scanned",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "The number of bytes scanned when the query was run",
		ConstLabels: prometheus.Labels{"account": account},
	}, queryLabels)

	elapsedTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "elapsed_time",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Elapsed time (in milliseconds)",
		ConstLabels: prometheus.Labels{"account": account},
		Buckets:     histogramBuckets,
	}, queryLabels)

	executionTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "execution_time",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Execution time (in milliseconds)",
		ConstLabels: prometheus.Labels{"account": account},
		Buckets:     histogramBuckets,
	}, queryLabels)

	compilationTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "compilation_time",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Compilation time (in milliseconds)",
		ConstLabels: prometheus.Labels{"account": account},
		Buckets:     histogramBuckets,
	}, queryLabels)

	rowsReturnedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "rows_returned",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Number of rows produced by this statement.",
		ConstLabels: prometheus.Labels{"account": account},
	}, queryLabels)

	queuedProvisionHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "queued_provision",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Time (in milliseconds) spent in the warehouse queue, waiting for the warehouse servers to provision, due to warehouse creation, resume, or resize.",
		ConstLabels: prometheus.Labels{"account": account},
		Buckets:     histogramBuckets,
	}, queryLabels)

	queuedRepairHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "queued_repair",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Time (in milliseconds) spent in the warehouse queue, waiting for servers in the warehouse to be repaired.",
		ConstLabels: prometheus.Labels{"account": account},
		Buckets:     histogramBuckets,
	}, queryLabels)

	queuedOverloadHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "queued_overload",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Time (in milliseconds) spent in the warehouse queue, due to the warehouse being overloaded by the current query workload.",
		ConstLabels: prometheus.Labels{"account": account},
		Buckets:     histogramBuckets,
	}, queryLabels)

	blockedTimeHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:        "blocked_time",
		Subsystem:   "query",
		Namespace:   "snowflake",
		Help:        "Time (in milliseconds) spent blocked by a concurrent DML.",
		ConstLabels: prometheus.Labels{"account": account},
		Buckets:     histogramBuckets,
	}, queryLabels)

	queryCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "count",
		Subsystem:   "query",
		Namespace:   "snowflake",
		ConstLabels: prometheus.Labels{"account": account},
	}, queryLabels)
)

// GatherQueryMetrics collects metrics about queries that have run
func GatherQueryMetrics(db *sql.DB, start chan time.Time, done chan bool) {
	for rangeStart := range start {
		end := rangeStart.Truncate(interval)
		start := end.Add(-interval)
		query := fmt.Sprintf("select * from table(information_schema.query_history(END_TIME_RANGE_START => to_timestamp_ltz('%s'), END_TIME_RANGE_END => to_timestamp_ltz('%s')));", start.Format(time.RFC3339), end.Format(time.RFC3339))
		log.Debug().Msgf("[QueryMetrics] Query: %s", query)
		if !dry {
			rows, err := runQuery(query, db)
			if err != nil {
				log.Error().Msgf("[QueryMetrics] Failed to query db for query history. %+v", err)
				done <- true
				continue
			}

			// notify parent thread that we finished our query
			done <- true

			queryInfo := &queryRow{}
			for rows.Next() {
				rows.StructScan(queryInfo)

				queryCounter.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Inc()

				if queryInfo.Status == "RUNNING" {
					log.Debug().Msg("[QueryMetrics] Skipping Running query since there aren't metrics for it")
					continue
				}

				bytesScannedCounter.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Add(queryInfo.BytesScanned)
				log.Debug().Msgf("[QueryMetrics] bytes_scanned:%v user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.BytesScanned, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				elapsedTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.ElapsedTime)
				executionTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.Executiontime)
				compilationTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.CompilationTime)
				log.Debug().Msgf("[QueryMetrics] elapsed_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.ElapsedTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debug().Msgf("[QueryMetrics] execution_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.Executiontime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debug().Msgf("[QueryMetrics] compilation_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.CompilationTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				rowsReturnedCounter.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Add(queryInfo.RowsProduced)
				log.Debug().Msgf("[QueryMetrics] rows_returned=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.RowsProduced, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				queuedProvisionHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.QueuedProvisioningTime)
				queuedRepairHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.QueuedRepairTime)
				queuedOverloadHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.QueuedOverloadTime)
				log.Debug().Msgf("[QueryMetrics] queued_provision=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.QueuedProvisioningTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debug().Msgf("[QueryMetrics] queued_repair=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.QueuedRepairTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debug().Msgf("[QueryMetrics] queued_overload=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.QueuedOverloadTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				blockedTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.TransactionBlockedTime)
				log.Debug().Msgf("[QueryMetrics] blocked_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.TransactionBlockedTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
			}

			rows.Close()
		} else {
			log.Info().Msg("[QueryMetrics] Skipping query execution due to presence of dry-run flag")
			done <- true
		}
	}
}
