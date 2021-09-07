#!/bin/sh

echo $IS_VERBOSE ;

python /sdap/granule_ingester/main.py \
  $([[ ! -z "$RABBITMQ_HOST" ]] && echo --rabbitmq-host=$RABBITMQ_HOST) \
  $([[ ! -z "$RABBITMQ_USERNAME" ]] && echo --rabbitmq-username=$RABBITMQ_USERNAME) \
  $([[ ! -z "$RABBITMQ_PASSWORD" ]] && echo --rabbitmq-password=$RABBITMQ_PASSWORD) \
  $([[ ! -z "$RABBITMQ_QUEUE" ]] && echo --rabbitmq-queue=$RABBITMQ_QUEUE) \
  $([[ ! -z "$CASSANDRA_CONTACT_POINTS" ]] && echo --cassandra-contact-points=$CASSANDRA_CONTACT_POINTS) \
  $([[ ! -z "$CASSANDRA_PORT" ]] && echo --cassandra-port=$CASSANDRA_PORT) \
  $([[ ! -z "$CASSANDRA_KEYSPACE" ]] && echo --cassandra-keyspace=$CASSANDRA_KEYSPACE) \
  $([[ ! -z "$CASSANDRA_USERNAME" ]] && echo --cassandra-username=$CASSANDRA_USERNAME) \
  $([[ ! -z "$CASSANDRA_PASSWORD" ]] && echo --cassandra-password=$CASSANDRA_PASSWORD) \
  $([[ ! -z "$METADATA_STORE" ]] && echo --metadata-store=$METADATA_STORE) \
  $([[ ! -z "$SOLR_HOST_AND_PORT" ]] && echo --solr-host-and-port=$SOLR_HOST_AND_PORT) \
  $([[ ! -z "$ZK_HOST_AND_PORT" ]] && echo --zk-host-and-port=$ZK_HOST_AND_PORT) \
  $([[ ! -z "$ELASTIC_URL" ]] && echo --elastic-url=$ELASTIC_URL) \
  $([[ ! -z "$ELASTIC_USERNAME" ]] && echo --elastic-username=$ELASTIC_USERNAME) \
  $([[ ! -z "$ELASTIC_PASSWORD" ]] && echo --elastic-password=$ELASTIC_PASSWORD) \
  $([[ ! -z "$ELASTIC_INDEX" ]] && echo --elastic-index=$ELASTIC_INDEX) \
  $([[ ! -z "$MAX_THREADS" ]] && echo --max-threads=$MAX_THREADS) \
  $([[ ! -z "$VERBOSE" ]] && echo --verbose)
  $([[ ! -z "$IS_VERBOSE" ]] && echo --verbose)
