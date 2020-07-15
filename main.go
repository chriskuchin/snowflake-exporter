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

	queryMetricsCollectChan   chan time.Time
	warehouseUsageCollectChan chan time.Time
	taskMetricsCollectChan    chan time.Time
	copyMetricsCollectChan    []chan time.Time
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
				queryMetricsCollectChan = make(chan time.Time)
				go GatherQueryMetrics(db, queryMetricsCollectChan, queriesDone)
			}

			if !disableWarehouseUsageCollection {
				jobCount++
				log.Debug("Enabling Warehouse Usage Metrics")
				warehouseUsageCollectChan = make(chan time.Time)
				go GatherWarehouseUsageMetrics(db, warehouseUsageCollectChan, queriesDone)
			}

			if !disableCopyMetricCollection {
				for _, table := range copyTables {
					jobCount++
					log.Debug("Enabling Copy Metrics")
					channel := make(chan time.Time)
					copyMetricsCollectChan = append(copyMetricsCollectChan, channel)
					go GatherCopyMetrics(table, db, channel, queriesDone)
				}
			}

			if !disableTaskMetricCollection {
				jobCount++
				log.Debug("Enabling Task Metrics")
				taskMetricsCollectChan = make(chan time.Time)
				go GatherTaskMetrics(db, taskMetricsCollectChan, queriesDone)
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
	var lastRun time.Time = time.Now()
	for {
		log.Debug("[Collect] Triggering a new collection cycle")
		loopStart := time.Now()
		trigger := lastRun
		if time.Now().Sub(lastRun) < interval {
			log.Debugf("[Collect] First run calculating %v ago.", interval)
			trigger = time.Now().Add(-interval).Add(-10 * time.Second)
		}
		lastRun = loopStart

		triggerCollectCycle(trigger)

		for a := 1; a <= jobCount; a++ {
			<-queriesDone
		}

		duration := time.Since(loopStart)
		exporterQueryCycleTime.Observe(float64(duration.Milliseconds()))
		log.Debugf("Execution of collect cycle took: %v", duration)
		time.Sleep(interval)
	}
}

func triggerCollectCycle(triggerTime time.Time) {
	if !disableCopyMetricCollection {
		for _, c := range copyMetricsCollectChan {
			c <- triggerTime
		}
	}

	if !disableWarehouseUsageCollection {
		warehouseUsageCollectChan <- triggerTime
	}

	if !disableQueryCollection {
		queryMetricsCollectChan <- triggerTime
	}

	if !disableTaskMetricCollection {
		taskMetricsCollectChan <- triggerTime
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
