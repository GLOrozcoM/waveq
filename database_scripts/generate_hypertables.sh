#!/bin/bash
# Turn ingested tables into hyper tables using TimescaleDB

# Activate extension
psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"

# Base table names
# - designate your database and table name for use
old_table="metrics_"
new_table="hyper_metrics_"
db_name="db_name"

for number in {1979..2010}
do
# - hyper table to place migrated data
psql -U postgres $db_name -c "CREATE TABLE $new_table$number (LIKE $old_table$number INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES);"

# - indicate the time column
psql -U postgres $db_name -c "SELECT create_hypertable('$new_table$number', 'time');"

# - migrate data
psql -U postgres $db_name -c "INSERT INTO $new_table$number SELECT * FROM $old_table$number;"
done
