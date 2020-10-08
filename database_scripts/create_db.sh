#!/bin/bash
# Tables in this db are a single location for a year.
psql -U postgres -c "CREATE DATABASE buoy_by_location;"
# Tables in this db are multiple locations per year.
psql -U postgres -c "CREATE DATABASE buoy_by_year;"
# Tables follow two formats:
# 1. A single variate table across multiple years.
# 2. A coordinates table to perform joins on.
# 3. A single power table for each year across 1000 locations.
psql -U postgres -c "CREATE DATABASE  hindcast;"
