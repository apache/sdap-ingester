
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
