#!/bin/bash

python /collection_manager/collection_manager/main.py \
  $([[ ! -z "$COLLECTIONS_PATH" ]] && echo --collections-path=$COLLECTIONS_PATH) \
  $([[ ! -z "$RABBITMQ_HOST" ]] && echo --rabbitmq-host=$RABBITMQ_HOST) \
  $([[ ! -z "$RABBITMQ_USERNAME" ]] && echo --rabbitmq-username=$RABBITMQ_USERNAME) \
  $([[ ! -z "$RABBITMQ_PASSWORD" ]] && echo --rabbitmq-password=$RABBITMQ_PASSWORD) \
  $([[ ! -z "$RABBITMQ_QUEUE" ]] && echo --rabbitmq-queue=$RABBITMQ_QUEUE) \
  $([[ ! -z "$HISTORY_URL" ]] && echo --history-url=$HISTORY_URL) \
  $([[ ! -z "$HISTORY_PATH" ]] && echo --history-path=$HISTORY_PATH) \
  $([[ ! -z "$REFRESH" ]] && echo --refresh=$REFRESH) \
  $([[ ! -z "$S3_BUCKET" ]] && echo --s3-bucket=$S3_BUCKET)
