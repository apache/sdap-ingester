
resource "aws_sqs_queue" "backfill_queue" {
  name                       = "cmr-subscriber-backfill-queue"
  message_retention_seconds  = 1209600
  policy                     = data.aws_iam_policy_document.queue_policy.json
  visibility_timeout_seconds = 1800
}

data "aws_iam_policy_document" "queue_policy" {
  statement {
    sid    = "__owner_statement"
    effect = "Allow"
    principals {
      identifiers = ["arn:${data.aws_partition.current.id}:iam::${data.aws_caller_identity.current.account_id}:root"]
      type        = "AWS"
    }
    actions   = ["SQS:*"]
    resources = ["arn:${data.aws_partition.current.id}:sqs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:cmr-subscriber-backfill-queue"]
  }
  statement {
    sid    = "CMR_subscription"
    effect = "Allow"
    principals {
      identifiers = ["sns.amazonaws.com"]
      type        = "Service"
    }
    actions   = ["SQS:SendMessage"]
    resources = ["arn:${data.aws_partition.current.id}:sqs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:cmr-subscriber-backfill-queue"]

    condition {
      test     = "StringEquals"
      values   = ["621933553860"]
      variable = "aws:SourceAccount"
    }

    condition {
      test     = "ArnLike"
      values   = ["arn:aws:sns:us-east-1:621933553860:cmr-subscriptions-prod"]
      variable = "aws:SourceArn"
    }
  }
}

resource "aws_lambda_event_source_mapping" "backfill_trigger" {
  function_name    = aws_lambda_function.lambda.arn
  event_source_arn = aws_sqs_queue.backfill_queue.arn

  enabled = var.enable_triggers

  batch_size              = 3
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 16
  }
}
