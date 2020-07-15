# Snowflake-Exporter
Prometheus exporter for [snowflake](https://www.snowflake.com/).

## Permissions
It is possible to create a read only role that has access to all necessary queries. Documentation on snowflake privileges can be found [here](https://docs.snowflake.com/en/user-guide/security-access-control-privileges.html).

| Privilege         | Object    | Required Metrics                                       |
|-------------------|-----------|--------------------------------------------------------|
| MONITOR EXECUTION | ACCOUNT   |                                                        |
| MONITOR USAGE     | ACCOUNT   |                                                        |
| MONITOR           | DATABASE  |                                                        |
| USAGE             | DATABASE  |                                                        |
| MONITOR           | SCHEMA    |                                                        |
| USAGE             | SCHEMA    |                                                        |
| MONITOR           | WAREHOUSE |                                                        |
| USAGE             | WAREHOUSE | Usage required for using warehouse to run queries with |

## Cost
I currently run this on X-SMALL warehouse and all queries finish in less 10s total. Because the minimum charge for a warehouse is 60s and we have a 60s auto suspend on the warehouse, We also run this at a 10m interval. This means every hour we pay for about 70s of execution time 6 times. or roughly ~.02 credits per execution. If query execution for metric collection continues to grow I plan on adding an Alter Warehouse permission to skip the auto_suspend and pin the cost to the initial 60s charge.

## Metrics

### Task Metrics
Task Metrics are collected by querying the [Task History (Table Function)](https://docs.snowflake.com/en/sql-reference/functions/task_history.html).

#### Future Work:
[] Handle more than 100 results in collect window

### Query Metrics
Query metrics are collected by query the [Query History (Table Function)](https://docs.snowflake.com/en/sql-reference/functions/query_history.html).

#### Future Work:
[] Handle more than the 100 result default limit

### Warehouse Metrics
Currently only warehouse billing metrics are collected by querying the [Warehouse Metering History (Table Function)](https://docs.snowflake.com/en/sql-reference/functions/warehouse_metering_history.html)

#### Future Work:
[] Add warehouse load metrics using [Warehouse Load History (Table Function)](https://docs.snowflake.com/en/sql-reference/functions/warehouse_load_history.html)

#### Copy Statement Metrics
Collected using the [Copy History (Table Function)](https://docs.snowflake.com/en/sql-reference/functions/copy_history.html). Collection of these metrics requires tables to be manually specified at exporter start.

#### Future Work
[] Investigate using the [LOAD HISTORY (View)](https://docs.snowflake.com/en/sql-reference/info-schema/load_history.html) to possibly remove table list requirements