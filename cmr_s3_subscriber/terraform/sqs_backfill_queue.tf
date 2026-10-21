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

  enabled = var.enable_backfill_trigger

  batch_size              = 3
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 16
  }
}
