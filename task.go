package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
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

func gatherTaskMetrics(db *sql.DB, start chan time.Time, done chan bool) {
	for rangeStart := range start {
		if !dry {
			query := fmt.Sprintf("select * from table(information_schema.task_history(scheduled_time_range_start => to_timestamp_ltz('%s'), scheduled_time_range_end => current_timestamp()));", rangeStart.Format(time.RFC3339))
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