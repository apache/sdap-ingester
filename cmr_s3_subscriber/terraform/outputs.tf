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


output "notification_topic" {
  value       = aws_sns_topic.subscriber_notify.arn
  description = "SNS topic for forwarding CMR subscription confirmations + error messages for unprocessable messages. Ensure this is subscribed to before creating subscriptions and/or enabling triggers."
}

output "backfill_queue" {
  value       = aws_sqs_queue.backfill_queue.arn
  description = "Special SQS queue that can be used to stage backfill data with the fill_queue_by_query.py script"
}

output "collection_path_lookup_table" {
  value       = aws_dynamodb_table.collection_lookup.arn
  description = "Lookup table to map collection short names to S3 paths to stage data to"
}

output "staging_bucket" {
  value       = local.bucket
  description = "S3 bucket data will be staged to"
}

output "subscription_info" {
  value = { for ccid, sub in module.subscriptions : ccid => sub.queue_arn }
}

output "triggers_enabled" {
  value = var.enable_triggers
}

output "maap_enabled" {
  value = var.maap_config != null
}
