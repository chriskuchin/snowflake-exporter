package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/snowflakedb/gosnowflake"
	_ "github.com/snowflakedb/gosnowflake"
)

var (
	user            string
	password        string
	account         string
	defaultDatabase string
	defaultSchema   string
	warehouse       string
	port            int
	path            string
	role            string
	interval        time.Duration

	disableQueryCollection          bool
	disableWarehouseUsageCollection bool
	disableTaskMetricCollection     bool
	disableCopyMetricCollection     bool

	debug bool
	dry   bool

	copyTables       []string
	histogramBuckets = []float64{0, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1250, 1500, 1750, 2000, 2500, 5000, 10000}

	queriesDone chan bool
	jobCount    int

	queryMetricsCollectChan   chan bool
	warehouseUsageCollectChan chan bool
	taskMetricsCollectChan    chan bool
	copyMetricsCollectChan    []chan bool
)

func main() {

	app := &cli.App{
		Name:    "Snowflake Exporter",
		Version: "1.0.0",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "copy-tables",
				Required: true,
				EnvVars: []string{
					"COPY_TABLES",
				},
			},
			&cli.StringFlag{
				Name:  "user",
				Usage: "Snowflake user to auth using",
				EnvVars: []string{
					"SNOWFLAKE_USER",
				},
				Required:    true,
				Destination: &user,
			},
			&cli.StringFlag{
				Name:  "password",
				Usage: "Snowflake password for authenticating the specified user with snowflake",
				EnvVars: []string{
					"SNOWFLAKE_PASSWORD",
				},
				Required:    true,
				Destination: &password,
			},
			&cli.StringFlag{
				Name:  "role",
				Usage: "The snowflake role to be used to run queries",
				EnvVars: []string{
					"SNOWFLAKE_ROLE",
				},
				Value:       "",
				Destination: &role,
			},
			&cli.StringFlag{
				Name:  "account",
				Usage: "The snowflake account the exporter should connect to",
				EnvVars: []string{
					"SNOWFLAKE_ACCOUNT",
				},
				Required:    true,
				Destination: &account,
			},
			&cli.StringFlag{
				Name:        "default-database",
				Usage:       "The database the initial connection is made against",
				Required:    true,
				Destination: &defaultDatabase,
				EnvVars: []string{
					"SNOWFLAKE_DEFAULT_DATABASE",
				},
			},
			&cli.StringFlag{
				Name:        "default-schema",
				DefaultText: "PUBLIC",
				Usage:       "The default schema for the initial connection",
				Destination: &defaultSchema,
				EnvVars: []string{
					"SNOWFLAKE_DEFAULT_SCHEMA",
				},
			},
			&cli.StringFlag{
				Name:        "warehouse",
				Usage:       "The warehouse to use for queries",
				Required:    true,
				Destination: &warehouse,
				EnvVars: []string{
					"SNOWFLAKE_WAREHOUSE",
				},
			},
			&cli.IntFlag{
				Name:        "port",
				Usage:       "the port to expose the metrics on",
				Value:       2112,
				Destination: &port,
				EnvVars: []string{
					"EXPORTER_PORT",
				},
			},
			&cli.StringFlag{
				Name:        "path",
				Usage:       "the path for the metrics endpoint",
				Value:       "/metrics",
				Destination: &path,
				EnvVars: []string{
					"EXPORTER_PATH",
				},
			},
			&cli.BoolFlag{
				Name:        "disable-query-metrics",
				Value:       false,
				Destination: &disableQueryCollection,
				EnvVars: []string{
					"DISABLE_QUERY_METRICS",
				},
			},
			&cli.BoolFlag{
				Name:        "disable-warehouse-usage-metrics",
				Value:       false,
				Destination: &disableWarehouseUsageCollection,
				EnvVars: []string{
					"DISABLE_WAREHOUSE_METRICS",
				},
			},
			&cli.BoolFlag{
				Name:        "disable-task-metrics",
				Value:       false,
				Destination: &disableTaskMetricCollection,
				EnvVars: []string{
					"DISABLE_TASK_METRICS",
				},
			},
			&cli.BoolFlag{
				Name:        "disable-copy-metrics",
				Value:       false,
				Destination: &disableCopyMetricCollection,
				EnvVars: []string{
					"DISABLE_COPY_METRICS",
				},
			},
			&cli.BoolFlag{
				Name:        "debug",
				Value:       false,
				Destination: &debug,
				EnvVars: []string{
					"DEBUG",
				},
			},
			&cli.BoolFlag{
				Name:        "dry-run",
				Value:       false,
				Destination: &dry,
				EnvVars: []string{
					"DRY_RUN",
				},
			},
			&cli.DurationFlag{
				Name:        "interval",
				Value:       10 * time.Minute,
				Destination: &interval,
				EnvVars: []string{
					"INTERVAL",
				},
			},
		},
		Action: func(c *cli.Context) error {
			if debug {
				log.Base().SetLevel("DEBUG")
			}

			registerMetrics()
			queriesDone = make(chan bool)
			copyTables = c.StringSlice("copy-tables")

			url, _ := gosnowflake.DSN(&gosnowflake.Config{
				Account:   account,
				User:      user,
				Password:  password,
				Database:  defaultDatabase,
				Schema:    defaultSchema,
				Warehouse: warehouse,
				Role:      role,
			})
			db, err := sql.Open("snowflake", url)
			if err != nil {
				log.Fatal("Failed to connect: ", err)
			}
			defer db.Close()

			if !disableQueryCollection {
				jobCount++
				log.Debug("Enabling Query Metrics")
				queryMetricsCollectChan = make(chan bool)
				go gatherQueryMetrics(db, queryMetricsCollectChan, queriesDone)
			}

			if !disableWarehouseUsageCollection {
				jobCount++
				log.Debug("Enabling Warehouse Usage Metrics")
				warehouseUsageCollectChan = make(chan bool)
				go gatherWarehouseMetrics(db, warehouseUsageCollectChan, queriesDone)
			}

			if !disableCopyMetricCollection {
				for _, table := range copyTables {
					jobCount++
					log.Debug("Enabling Copy Metrics")
					channel := make(chan bool)
					copyMetricsCollectChan = append(copyMetricsCollectChan, channel)
					go gatherCopyMetrics(table, db, channel, queriesDone)
				}
			}

			if !disableTaskMetricCollection {
				jobCount++
				log.Debug("Enabling Task Metrics")
				taskMetricsCollectChan = make(chan bool)
				go gatherTaskMetrics(db, taskMetricsCollectChan, queriesDone)
			}

			go collect()

			log.Debugf("Starting metrics server on port: %d path: %s", port, path)
			http.Handle(path, promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", port), nil)

			return err
		},
	}
	app.Run(os.Args)
}

func collect() {
	for {
		start := time.Now()
		triggerCollectCycle()

		for a := 1; a <= jobCount; a++ {
			<-queriesDone
		}

		// suspend the warehouse

		duration := time.Since(start)
		exporterQueryCycleTime.Observe(float64(duration.Milliseconds()))
		log.Debugf("Execution of collect cycle took: %v", duration)
		time.Sleep(interval)
	}
}

func triggerCollectCycle() {
	if !disableCopyMetricCollection {
		for _, c := range copyMetricsCollectChan {
			c <- true
		}
	}

	if !disableWarehouseUsageCollection {
		warehouseUsageCollectChan <- true
	}

	if !disableQueryCollection {
		queryMetricsCollectChan <- true
	}

	if !disableTaskMetricCollection {
		taskMetricsCollectChan <- true
	}
}

var (
	exporterQueryCycleTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "query_time_ms",
		Subsystem:   "exporter",
		Namespace:   "snowflake",
		Help:        "Total time the exporter spent running queries against snowflake",
		ConstLabels: prometheus.Labels{"account": account},
	})
)

func registerMetrics() {
	prometheus.MustRegister(exporterQueryCycleTime)
	// Query Metrics
	prometheus.MustRegister(bytesScannedCounter, rowsReturnedCounter, elapsedTimeHistogram, executionTimeHistogram, compilationTimeHistogram, queuedProvisionHistogram, queuedRepairHistogram, queuedOverloadHistogram, blockedTimeHistogram, queryCounter)

	// Copy Metrics
	prometheus.MustRegister(rowsLoadedCounter, errorRowCounter, parsedRowCounter, copyCounter, successGauge)

	// Task Metrics
	prometheus.MustRegister(taskRunCounter)

	// Warehouse Usage Metrics
	prometheus.MustRegister(warehouseTotalCreditsUsed, warehouseCloudCreditsUsed, warehouseComputeCreditsUsed)
}

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

func gatherQueryMetrics(db *sql.DB, start chan bool, done chan bool) {
	for range start {
		if !dry {
			query := fmt.Sprintf("select * from table(information_schema.query_history(END_TIME_RANGE_START=>DATEADD(minutes, -%f, CURRENT_TIMESTAMP())));", interval.Minutes())
			log.Debugf("[QueryMetrics] Query: %s", query)
			rows, err := runQuery(query, db)
			if err != nil {
				log.Errorf("[QueryMetrics] Failed to query db for query history. %+v", err)
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
					log.Debug("[QueryMetrics] Skipping Running query since there aren't metrics for it")
					continue
				}

				bytesScannedCounter.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Add(queryInfo.BytesScanned)
				log.Debugf("[QueryMetrics] bytes_scanned:%v user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.BytesScanned, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				elapsedTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.ElapsedTime)
				executionTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.Executiontime)
				compilationTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.CompilationTime)
				log.Debugf("[QueryMetrics] elapsed_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.ElapsedTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debugf("[QueryMetrics] execution_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.Executiontime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debugf("[QueryMetrics] compilation_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.CompilationTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				rowsReturnedCounter.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Add(queryInfo.RowsProduced)
				log.Debugf("[QueryMetrics] rows_returned=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.RowsProduced, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				queuedProvisionHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.QueuedProvisioningTime)
				queuedRepairHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.QueuedRepairTime)
				queuedOverloadHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.QueuedOverloadTime)
				log.Debugf("[QueryMetrics] queued_provision=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.QueuedProvisioningTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debugf("[QueryMetrics] queued_repair=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.QueuedRepairTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
				log.Debugf("[QueryMetrics] queued_overload=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.QueuedOverloadTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)

				blockedTimeHistogram.WithLabelValues(queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status).Observe(queryInfo.TransactionBlockedTime)
				log.Debugf("[QueryMetrics] blocked_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", queryInfo.TransactionBlockedTime, queryInfo.User, queryInfo.Warehouse, queryInfo.Schema, queryInfo.Database, queryInfo.Status)
			}

			rows.Close()
		} else {
			log.Info("[QueryMetrics] Skipping query execution due to presence of dry-run flag")
			done <- true
		}
	}
}

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

func gatherCopyMetrics(table string, db *sql.DB, start chan bool, done chan bool) {
	for range start {
		if !dry {
			query := fmt.Sprintf("select * from table(information_schema.copy_history(TABLE_NAME => '%s', START_TIME=> to_timestamp_ltz('%s'), END_TIME => current_timestamp()));", table, time.Now().Add(-interval).Format(time.RFC3339))
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

type task struct {
	Name     string `db:"NAME"`
	Database string `db:"DATABASE_NAME"`
	Schema   string `db:"SCHEMA_NAME"`
	State    string `db:"STATE"`
}

var (
	taskLabels     = []string{"state", "task", "schema", "database"}
	taskRunCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "run_count",
		Subsystem:   "task",
		Namespace:   "snowflake",
		Help:        "Number of time the task has run",
		ConstLabels: prometheus.Labels{"account": account},
	}, taskLabels)
)

func gatherTaskMetrics(db *sql.DB, start chan bool, done chan bool) {
	for range start {
		if !dry {
			query := fmt.Sprintf("select * from table(information_schema.task_history(scheduled_time_range_start => to_timestamp_ltz('%s'), scheduled_time_range_end => current_timestamp()));", time.Now().Add(-interval).Format(time.RFC3339))
			log.Debugf("[TaskMetrics] Query: %s", query)
			rows, err := runQuery(query, db)
			if err != nil {
				log.Errorf("Failed to query db for task history. %+v", err)
				done <- true
				continue
			}

			done <- true

			task := &task{}
			for rows.Next() {
				rows.StructScan(task)

				// skip tasks that will run in the future
				if task.State == "SCHEDULED" {
					continue
				}

				taskRunCounter.WithLabelValues(task.State, task.Name, task.Schema, task.Database).Inc()
			}

			rows.Close()
		} else {
			log.Info("[TaskMetrics] Skipping query execution due to presence of dry-run flag")
			done <- true
		}
	}
}

var (
	warehouseTotalCreditsUsed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "credits_total",
		Subsystem: "warehouse",
		Namespace: "snowflake",
		Help:      "Total credits consumed for the past hour by the particular warehouse",
	}, []string{"warehouse"})

	warehouseCloudCreditsUsed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "credits_cloud",
		Subsystem: "warehouse",
		Namespace: "snowflake",
		Help:      "Total cloud credits consumed by the warehouse in the past hour",
	}, []string{"warehouse"})

	warehouseComputeCreditsUsed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "credits_compute",
		Subsystem: "warehouse",
		Namespace: "snowflake",
		Help:      "Total compute credits used in the last timeframe",
	}, []string{"warehouse"})
)

type warehouseBilling struct {
	Warehouse          string  `db:"WAREHOUSE_NAME"`
	CreditsUsed        float64 `db:"CREDITS_USED"`
	CreditsUsedCompute float64 `db:"CREDITS_USED_COMPUTE"`
	CreditsUsedCloud   float64 `db:"CREDITS_USED_CLOUD_SERVICES"`
}

// Need to specify the list of warehouses to monitor
func gatherWarehouseMetrics(db *sql.DB, start chan bool, done chan bool) {
	var lastRun time.Time = time.Now()
	for range start {
		if !dry {
			loopStart := time.Now()
			start := lastRun
			if time.Now().Sub(lastRun) < interval {
				log.Debugf("[WarehouseUsage] First run calculating %v ago.", interval)
				start = time.Now().Add(-interval)
			}
			lastRun = loopStart
			query := fmt.Sprintf("select * from table(information_schema.warehouse_metering_history(DATE_RANGE_START => to_timestamp_ltz('%s'), DATE_RANGE_END => current_timestamp()));", start.Add(-30*time.Second).Format(time.RFC3339))
			log.Debugf("[WarehouseUsage] Query: %s", query)
			rows, err := runQuery(query, db)
			if err != nil {
				log.Errorf("[WarehouseUsage] Failed to gather warehouse metrics: %+v\n", err)
				done <- true
				continue
			}

			done <- true

			warehouse := &warehouseBilling{}
			for rows.Next() {
				rows.StructScan(warehouse)

				log.Debugf("[WarehouseUsage] row: %+v", warehouse)

				warehouseCloudCreditsUsed.WithLabelValues(warehouse.Warehouse).Add(warehouse.CreditsUsedCloud)
				warehouseComputeCreditsUsed.WithLabelValues(warehouse.Warehouse).Add(warehouse.CreditsUsedCompute)
				warehouseTotalCreditsUsed.WithLabelValues(warehouse.Warehouse).Add(warehouse.CreditsUsed)
			}

			rows.Close()
		} else {
			log.Info("[WarehouseUsage] Skipping query execution due to presence of dry-run flag")
			done <- true
		}
	}
}

func runQuery(query string, db *sql.DB) (*sqlx.Rows, error) {
	unsafe := sqlx.NewDb(db, "snowflake").Unsafe()
	rows, err := unsafe.Queryx(query)

	return rows, err
}

func dumpQueryResults(rows *sqlx.Rows) {
	columns, _ := rows.Columns()

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	log.Info(columns)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		rows.Scan(scanArgs...)
		var value string
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			fmt.Println(columns[i], ": ", value)
		}
		fmt.Println("-----------------------------------") // rows.StructScan(copy)

	}
}
