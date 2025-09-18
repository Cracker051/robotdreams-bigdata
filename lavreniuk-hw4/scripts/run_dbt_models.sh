#!/bin/bash
# We use DBT-core, so we additionaly need to write python script for email sendint. DBT-cloud has built-in instrument for this.
# Also, for DBT-core add this script to crontab with 0 13 * * * ./scripts/run_dbt.sh

cd hw4_dbt
dbt run && dbt test
STATUS=$?

if [ $STATUS -ne 0 ]; then
    echo "dbt run/test failed"
else
    echo "dbt run/test succeeded"
fi
