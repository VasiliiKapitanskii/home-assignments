#!/usr/bin/env bash

set -x

airflow db check-migrations

if [ $? -ne 0 ]; then
  airflow db init

  airflow users create \
    --email airflow --firstname airflow \
    --lastname airflow --password airflow \
    --role Admin --username airflow

fi

CONNECTIONS_FILE=${CONNECTIONS_FILE:-local_connections.yaml}
OLD_CONNECTIONS=`airflow connections list -o json | jq -r '.[].conn_id'`

for conn_id in $OLD_CONNECTIONS ; do
    airflow connections delete --color off $conn_id
done

airflow connections import $CONNECTIONS_FILE


echo "DONE!"
