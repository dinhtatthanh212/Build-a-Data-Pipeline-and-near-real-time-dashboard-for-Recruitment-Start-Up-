#!/bin/sh
#source /.env
#echo "DB_HOST=$DB_HOST"
#echo "DB_PORT=$DB_PORT"
#echo "DB_NAME=$DB_NAME"
#echo "MQTT_HOST=$MQTT_HOST"
#echo "MQTT_PORT=$MQTT_PORT"
#echo "CASSANDRA_HOST=$CASSANDRA_HOST"
mkdir /tmp/.ivy
exec /opt/spark/bin/spark-submit \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \ETL_Pipeline.py
#--conf spark.cassandra.connection.host=$CASSANDRA_HOST \#
#--conf spark.jars.ivy=/tmp/.ivy \#
#--conf spark.sql.shuffle.partitions=50 \
#--conf spark.executor.memory=2g \
#--conf spark.driver.cores=1  \
#--conf spark.ui.retainedJobs=200 \
#--conf --num-executors=1 \
#/ETL_Pipeline.py
