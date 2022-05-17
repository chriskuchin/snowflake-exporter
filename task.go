package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

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

// GatherTaskMetrics collects metrics about executed tasks
func GatherTaskMetrics(db *sql.DB, start chan time.Time, done chan bool) {
	for rangeStart := range start {
		end := rangeStart.Truncate(interval)
		start := end.Add(-interval)
		query := fmt.Sprintf("select * from table(information_schema.task_history(scheduled_time_range_start => to_timestamp_ltz('%s'), scheduled_time_range_end => to_timestamp_ltz('%s')));", start.Format(time.RFC3339), end.Format(time.RFC3339))
		log.Debug().Msgf("[TaskMetrics] Query: %s", query)
		if !dry {
			rows, err := runQuery(query, db)
			if err != nil {
				log.Error().Msgf("Failed to query db for task history. %+v", err)
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
			log.Info().Msg("[TaskMetrics] Skipping query execution due to presence of dry-run flag")
			done <- true
		}
	}
}
