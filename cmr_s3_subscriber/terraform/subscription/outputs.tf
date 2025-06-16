output "queue_arn" {
  value = aws_sqs_queue.queue.arn
}

output "ccid" {
  value = null_resource.create_subscription.triggers.ccid
}
