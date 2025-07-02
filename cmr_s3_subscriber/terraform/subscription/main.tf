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


data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "aws_partition" "current" {}


resource "aws_sqs_queue" "queue" {
  name = "cmr-subscriber-${var.ccid}"
  message_retention_seconds = 1209600
  policy = data.aws_iam_policy_document.queue_policy.json
  visibility_timeout_seconds = 1800
}

data "aws_iam_policy_document" "queue_policy" {
  statement {
    sid = "__owner_statement"
    effect = "Allow"
    principals {
      identifiers = ["arn:${data.aws_partition.current.id}:iam::${data.aws_caller_identity.current.account_id}:root"]
      type = "AWS"
    }
    actions = ["SQS:*"]
    resources = ["arn:${data.aws_partition.current.id}:sqs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:cmr-subscriber-${var.ccid}"]
  }
  statement {
    sid = "CMR_subscription"
    effect = "Allow"
    principals {
      identifiers = ["sns.amazonaws.com"]
      type = "Service"
    }
    actions = ["SQS:SendMessage"]
    resources = ["arn:${data.aws_partition.current.id}:sqs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:cmr-subscriber-${var.ccid}"]

    condition {
      test     = "StringEquals"
      values = ["621933553860"]
      variable = "aws:SourceAccount"
    }

    condition {
      test     = "ArnLike"
      values = ["arn:aws:sns:us-east-1:621933553860:cmr-subscriptions-prod"]
      variable = "aws:SourceArn"
    }
  }
}

resource "null_resource" "create_subscription" {
  depends_on = [aws_sqs_queue.queue]

  triggers = {
    script_dir = var.script_dir
    ccid = var.ccid
    config_file = var.config_file
  }

  provisioner "local-exec" {
    command = "source venv/bin/activate; python subscriber.py ${var.config_file} ${var.ccid} --queue ${aws_sqs_queue.queue.arn}"
    working_dir = var.script_dir
    interpreter = ["/bin/bash", "-c"]
  }

  provisioner "local-exec" {
    command = "source venv/bin/activate; python delete.py ${self.triggers.config_file} --response-xml subscriptions/${self.triggers.ccid}-subscription.xml"
    working_dir = self.triggers.script_dir
    interpreter = ["/bin/bash", "-c"]
    when = destroy
    on_failure = continue
  }
}
