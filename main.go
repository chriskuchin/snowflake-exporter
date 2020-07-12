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

	copyTables       []string
	histogramBuckets = []float64{0, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1250, 1500, 1750, 2000, 2500, 5000, 10000}
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
				log.Debug("Enabling Query Metrics")
				go gatherQueryMetrics(db)
			}

			if !disableWarehouseUsageCollection {
				log.Debug("Enabling Warehouse Usage Metrics")
				go gatherWarehouseMetrics(db)
			}

			if !disableCopyMetricCollection {
				log.Debug("Enabling Copy Metrics")
				go gatherCopyMetrics(db)
			}

			if !disableTaskMetricCollection {
				log.Debug("Enabling Task Metrics")
				go gatherTaskMetrics(db)
			}

			log.Debugf("Starting metrics server on port: %d path: %s", port, path)
			http.Handle(path, promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", port), nil)

			return err
		},
	}
	app.Run(os.Args)
}

type query struct {
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

func gatherQueryMetrics(db *sql.DB) {
	prometheus.MustRegister(bytesScannedCounter, rowsReturnedCounter, elapsedTimeHistogram, executionTimeHistogram, compilationTimeHistogram, queuedProvisionHistogram, queuedRepairHistogram, queuedOverloadHistogram, blockedTimeHistogram, queryCounter)

	for {
		rows, err := runQuery(fmt.Sprintf("select * from table(information_schema.query_history(END_TIME_RANGE_START=>DATEADD(minutes, -%f, CURRENT_TIMESTAMP())));", interval.Minutes()), db)
		if err != nil {
			log.Errorf("Failed to query db for query history. %+v", err)
			time.Sleep(1 * time.Minute)
			continue
		}

		query := &query{}
		for rows.Next() {
			rows.StructScan(query)

			queryCounter.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Inc()

			if query.Status == "RUNNING" {
				log.Debug("Skipping Running query since there aren't metrics for it")
				time.Sleep(1 * time.Minute)
				continue
			}

			bytesScannedCounter.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Add(query.BytesScanned)
			log.Debugf("bytes_scanned:%v user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.BytesScanned, query.User, query.Warehouse, query.Schema, query.Database, query.Status)

			elapsedTimeHistogram.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Observe(query.ElapsedTime)
			executionTimeHistogram.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Observe(query.Executiontime)
			compilationTimeHistogram.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Observe(query.CompilationTime)
			log.Debugf("elapsed_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.ElapsedTime, query.User, query.Warehouse, query.Schema, query.Database, query.Status)
			log.Debugf("execution_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.Executiontime, query.User, query.Warehouse, query.Schema, query.Database, query.Status)
			log.Debugf("compilation_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.CompilationTime, query.User, query.Warehouse, query.Schema, query.Database, query.Status)

			rowsReturnedCounter.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Add(query.RowsProduced)
			log.Debugf("rows_returned=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.RowsProduced, query.User, query.Warehouse, query.Schema, query.Database, query.Status)

			queuedProvisionHistogram.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Observe(query.QueuedProvisioningTime)
			queuedRepairHistogram.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Observe(query.QueuedRepairTime)
			queuedOverloadHistogram.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Observe(query.QueuedOverloadTime)
			log.Debugf("queued_provision=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.QueuedProvisioningTime, query.User, query.Warehouse, query.Schema, query.Database, query.Status)
			log.Debugf("queued_repair=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.QueuedRepairTime, query.User, query.Warehouse, query.Schema, query.Database, query.Status)
			log.Debugf("queued_overload=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.QueuedOverloadTime, query.User, query.Warehouse, query.Schema, query.Database, query.Status)

			blockedTimeHistogram.WithLabelValues(query.User, query.Warehouse, query.Schema, query.Database, query.Status).Observe(query.TransactionBlockedTime)
			log.Debugf("blocked_time=%v: user: %s, warehouse: %s, schema: %s, database: %s, status: %s\n", query.TransactionBlockedTime, query.User, query.Warehouse, query.Schema, query.Database, query.Status)
		}

		rows.Close()
		time.Sleep(interval)
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

//  ROW_COUNT ROW_PARSED FILE_SIZE FIRST_ERROR_MESSAGE FIRST_ERROR_LINE_NUMBER FIRST_ERROR_CHARACTER_POS ERROR_COUNT ERROR_LIMIT STATUS  TABLE_SCHEMA_NAME TABLE_NAME PIPE_CATALOG_NAME PIPE_SCHEMA_NAME PIPE_NAME PIPE_RECEIVED_TIME

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

func gatherCopyMetrics(db *sql.DB) {
	prometheus.MustRegister(rowsLoadedCounter, errorRowCounter, parsedRowCounter, copyCounter, successGauge)

	for {
		for _, table := range copyTables {
			query := fmt.Sprintf("select * from table(information_schema.copy_history(TABLE_NAME=>'%s', START_TIME=> DATEADD(minute, -%f, CURRENT_TIMESTAMP())));", table, interval.Minutes())
			rows, err := runQuery(query, db)
			if err != nil {
				log.Errorf("Failed to query db for copy history. %+v", err)
				time.Sleep(1 * time.Minute)
				continue
			}

			copy := &copy{}
			for rows.Next() {
				rows.StructScan(copy)
				log.Debug("CopyHistory: ", copy)

				copyCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Inc()
				if copy.Status == "LOADED" {
					successGauge.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Set(1)
				}

				rowsLoadedCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Add(copy.RowCount)
				errorRowCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Add(copy.ErrorCount)
				parsedRowCounter.WithLabelValues(copy.Table, copy.Schema, copy.Database, copy.Status).Add(copy.RowParsed)
			}

			rows.Close()
		}
		time.Sleep(interval)
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

func gatherTaskMetrics(db *sql.DB) {
	prometheus.MustRegister(taskRunCounter)
	for {
		rows, err := runQuery(fmt.Sprintf("select * from table(information_schema.task_history(scheduled_time_range_start=>dateadd('minute',-%f,current_timestamp())));", interval.Minutes()), db)
		if err != nil {
			log.Errorf("Failed to query db for task history. %+v", err)
			time.Sleep(1 * time.Minute)
			continue
		}

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
		time.Sleep(interval)
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
func gatherWarehouseMetrics(db *sql.DB) {
	prometheus.MustRegister(warehouseTotalCreditsUsed, warehouseCloudCreditsUsed, warehouseComputeCreditsUsed)
	for {
		rows, err := runQuery(fmt.Sprintf("select * from table(information_schema.warehouse_metering_history(DATE_RANGE_START => dateadd('minute',-%f,current_timestamp())));", interval.Minutes()), db)
		if err != nil {
			log.Errorf("Failed to gather warehouse metrics: %+v\n", err)
			time.Sleep(1 * time.Minute)
			return
		}

		log.Debug("Processing warehouse billing")
		warehouse := &warehouseBilling{}
		for rows.Next() {
			rows.StructScan(warehouse)

			log.Debug("Warehouse-metering: ", warehouse)

			warehouseCloudCreditsUsed.WithLabelValues(warehouse.Warehouse).Add(warehouse.CreditsUsedCloud)
			warehouseComputeCreditsUsed.WithLabelValues(warehouse.Warehouse).Add(warehouse.CreditsUsedCompute)
			warehouseTotalCreditsUsed.WithLabelValues(warehouse.Warehouse).Add(warehouse.CreditsUsed)
		}

		rows.Close()
		time.Sleep(interval)
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
