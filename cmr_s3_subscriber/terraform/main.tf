
resource "aws_dynamodb_table" "collection_lookup" {
  name         = "cmr-subscriber-collections"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "collection"

  attribute {
    name = "collection"
    type = "S"
  }
}

resource "aws_sns_topic" "subscriber_notify" {
  name = "cmr-subscriber-notify"
}

resource "aws_sns_topic_policy" "topic_policy" {
  arn    = aws_sns_topic.subscriber_notify.arn
  policy = data.aws_iam_policy_document.sns_topic_policy.json
}

data "aws_iam_policy_document" "sns_topic_policy" {
  policy_id = "__default_policy_ID"
  version   = "2008-10-17"

  statement {
    actions = [
      "SNS:GetTopicAttributes",
      "SNS:SetTopicAttributes",
      "SNS:AddPermission",
      "SNS:RemovePermission",
      "SNS:DeleteTopic",
      "SNS:Subscribe",
      "SNS:ListSubscriptionsByTopic",
      "SNS:Publish"
    ]

    condition {
      test = "StringEquals"
      values = [
        data.aws_caller_identity.current.account_id
      ]
      variable = "AWS:SourceOwner"
    }

    effect = "Allow"

    principals {
      identifiers = ["*"]
      type        = "AWS"
    }

    resources = [
      aws_sns_topic.subscriber_notify.arn
    ]

    sid = "__default_statement_ID"
  }
}


resource "aws_secretsmanager_secret" "edl_secret" {
  name                    = "cmr-subscription-secrets"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "edl_secret" {
  secret_id = aws_secretsmanager_secret.edl_secret.id
  secret_string = jsonencode(merge(
    {
      EARTHDATA_USER     = var.edl_username
      EARTHDATA_PASSWORD = var.edl_password
    },
    var.maap_config != null ? var.maap_config : {},
    var.maap_kwargs != null ? { for key, value in var.maap_kwargs : "_maap_kwarg_${key}" => value } : {},
  ))
}

data "local_file" "lambda_package" {
  filename = var.maap_config == null ? "../lambda/package.zip" : "../lambda/package_maap.zip"
}

resource "aws_lambda_function" "lambda" {
  function_name    = "cmr-subscriber"
  role             = aws_iam_role.lambda_role.arn
  architectures    = ["x86_64"]
  filename         = data.local_file.lambda_package.filename
  source_code_hash = data.local_file.lambda_package.content_base64sha256
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  memory_size      = 512
  ephemeral_storage {
    size = 2048
  }
  timeout     = 900
  description = "Function to handle incoming messages from CMR subscriptions."

  environment {
    variables = {
      SNS_ARN    = aws_sns_topic.subscriber_notify.arn
      DDB_ARN    = aws_dynamodb_table.collection_lookup.arn
      SECRET_ARN = aws_secretsmanager_secret.edl_secret.arn
      DST_BUCKET = local.bucket
    }
  }

  vpc_config {
    security_group_ids = var.lambda_vpc.security_groups
    subnet_ids         = var.lambda_vpc.subnet_ids
  }

  reserved_concurrent_executions = 16
}

resource "local_file" "subscriber_config_file" {
  filename = "config.yaml"
  content = yamlencode({
    edl_username = var.edl_username
    edl_password = var.edl_password
  })
}

module "subscriptions" {
  source   = "./subscription"
  for_each = toset(var.ccids)

  ccid        = each.value
  config_file = local_file.subscriber_config_file.filename
  script_dir  = "/Users/rileykk/FireAlarm/subscriber"
}

resource "aws_lambda_event_source_mapping" "sqs_triggers" {
  function_name    = aws_lambda_function.lambda.arn
  for_each         = module.subscriptions
  event_source_arn = each.value.queue_arn

  enabled = var.enable_triggers

  batch_size              = 3
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 16
  }
}
