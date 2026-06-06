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


variable "existing_bucket" {
  type        = string
  description = "Existing S3 bucket to use instead of provisioning a new one"
  nullable    = true
}

variable "bucket_name_prefix" {
  type        = string
  description = "Prefix of the S3 bucket name to provision. Will be appended with a random string"
}

variable "edl_username" {
  type        = string
  description = "Username for Earthdata Login"
}

variable "edl_password" {
  type        = string
  description = "Password for Earthdata Login"
  sensitive   = true
}

variable "project" {
  type        = string
  description = "Name of the project this is deployed to. Used for notification headers."
  default     = null
}

variable "script_dir" {
  type        = string
  description = "Path to directory containing CMR subscription scripts and python venv"

  validation {
    condition     = fileexists(join("/", [trimsuffix(pathexpand(var.script_dir), "/"), "subscriber.py"]))
    error_message = "Cannot find subscriber script in script_dir"
  }

  validation {
    condition     = fileexists(join("/", [trimsuffix(pathexpand(var.script_dir), "/"), "delete.py"]))
    error_message = "Cannot find subscription deletion script in script_dir"
  }

  validation {
    condition     = fileexists(join("/", [trimsuffix(pathexpand(var.script_dir), "/"), "venv", "bin", "python"]))
    error_message = "Cannot find python venv in script_dir (expected to be in script_dir/venv)"
  }
}

variable "ccids" {
  type        = list(string)
  description = "List of CMR collection-concept-IDs to subscribe to. Note: Initial apply should have either no CCIDs listed or triggers disabled until the notification SNS topic is subscribed to"
}

variable "collection_options" {
  type = map(object({
    shortname               = string
    s3_path                 = optional(string)
    polygon                 = optional(string)
    trigger_on_revisions    = optional(bool, true)
    trigger_enable_override = optional(bool)
    delay                   = optional(number, 0)
    maap_config             = optional(map(string))
  }))
  description = "Mapping of CCID to collection options. If specified, must provide the short name of the collection plus an s3 path and/or MAAP options. MAAP options consist of an S3 URL for job configuration and an optional list of variables (either '*' or a space-separated list wrapped in quotes)"

  # TODO: Validations
}

variable "enable_triggers" {
  type        = bool
  description = "Whether to enable the SQS -> Lambda triggers. Note: Initial apply should have either no CCIDs listed or triggers disabled until the notification SNS topic is subscribed to"

  default = false
}

variable "enable_backfill_trigger" {
  type        = bool
  description = "Whether to enable the SQS -> Lambda trigger for the backfill queue"

  default = true
}

variable "lambda_vpc" {
  type = object({
    subnet_ids      = list(string)
    security_groups = list(string)
  })
  default = {
    subnet_ids      = []
    security_groups = []
  }

  description = "If VPC is required for the lambda function, list desired subnets & security groups here."

  validation {
    condition     = (length(var.lambda_vpc.subnet_ids) == 0 && length(var.lambda_vpc.security_groups) == 0) || (length(var.lambda_vpc.subnet_ids) > 0 && length(var.lambda_vpc.security_groups) > 0)
    error_message = "Must provide at least one of both SG & subnet or neither"
  }
}

variable "maap_config" {
  type = object({
    MAAP_ALGO_ID      = string
    MAAP_ALGO_VERSION = string
    MAAP_QUEUE        = string
    MAAP_PGT          = string
  })
  default = null

  description = "If a MAAP job is desired to stage the data rather than the bundled lambda function, specify the necessary config values for MAAP access and the localization algorithm."
}

variable "maap_kwargs" {
  type    = map(string)
  default = null

  description = "Define any additional kwargs needed by the MAAP job here. kwargs already defined in code are granule_id = GranuleUR & collection_id = CCID"
}

