#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
