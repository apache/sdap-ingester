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


data "aws_iam_policy_document" "assume_policy" {
  statement {
    effect = "Allow"

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "lambda_policy" {
  statement {
    sid       = "LambdaInvoke"
    actions   = ["lambda:InvokeFunction"]
    effect    = "Allow"
    resources = ["arn:${data.aws_partition.current.id}:lambda:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:function:*"]
  }

  statement {
    sid       = "Secret"
    actions   = ["secretsmanager:GetSecretValue"]
    effect    = "Allow"
    resources = [aws_secretsmanager_secret.edl_secret.arn]
  }

  statement {
    sid = "DynamoDB"
    actions = [
      "dynamodb:DescribeStream",
      "dynamodb:DescribeTable",
      "dynamodb:Get*",
      "dynamodb:Query"
    ]
    effect    = "Allow"
    resources = [aws_dynamodb_table.collection_lookup.arn]
  }

  statement {
    sid = "SNS"
    actions = [
      "sns:Get*",
      "sns:List*",
      "sns:Publish"
    ]
    effect    = "Allow"
    resources = [aws_sns_topic.subscriber_notify.arn]
  }

  statement {
    sid = "SQS"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    effect    = "Allow"
    resources = ["arn:${data.aws_partition.current.id}:sqs:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:cmr-subscriber-*"]
  }

  statement {
    sid = "S3"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    effect = "Allow"
    resources = [
      "arn:${data.aws_partition.current.id}:s3:::${local.bucket}",
      "arn:${data.aws_partition.current.id}:s3:::${local.bucket}/*"
    ]
  }
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "cmr-subscriber-lambda-policy"
  description = "Custom permissions for CMR subscriber lambda function"
  policy      = data.aws_iam_policy_document.lambda_policy.json
}

resource "aws_iam_role" "lambda_role" {
  assume_role_policy = data.aws_iam_policy_document.assume_policy.json
  name               = "cmr-subscriber-lambda-role"
}

resource "aws_iam_role_policy_attachment" "lambda_role_policy_attach" {
  for_each = tomap({
    main_policy  = aws_iam_policy.lambda_policy.arn,
    aws_policy_0 = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    aws_policy_1 = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
  })
  policy_arn = each.value
  role       = aws_iam_role.lambda_role.name
}

