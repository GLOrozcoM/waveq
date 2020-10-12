#!/bin/bash
# Lat long comes in from Spark in double precision format.
# -> Change to numeric to allow for precise and reliable location comparisons.

# Designate your own tables and database
table_name="base_name_"
db_name="db_vars"

# Years 1979 to 2010
for i in {1979..2010}
do
psql -U -d $db_name -c "ALTER TABLE $table_name$i ALTER COLUMN lat TYPE numeric";
psql -U -d $db_name -c "ALTER TABLE $table_name$i ALTER COLUMN long TYPE numeric";
done

