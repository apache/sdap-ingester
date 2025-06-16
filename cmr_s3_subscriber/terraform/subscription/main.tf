
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
    command = "source venv/bin/activate; python subscriber.py ${var.config_file} ${var.ccid} --queue ${aws_sqs_queue.queue.arn} --dryrun; cp subscriptions/C2930763263-LARC_CLOUD-subscription.xml subscriptions/${var.ccid}-subscription.xml"
    working_dir = var.script_dir
    interpreter = ["/bin/bash", "-c"]
  }

  provisioner "local-exec" {
    command = "source venv/bin/activate; python delete.py ${self.triggers.config_file} --response-xml subscriptions/${self.triggers.ccid}-subscription.xml --dryrun"
    working_dir = self.triggers.script_dir
    interpreter = ["/bin/bash", "-c"]
    when = destroy
    on_failure = continue
  }
}
