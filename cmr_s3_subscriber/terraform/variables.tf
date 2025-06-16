
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

variable "ccids" {
  type        = list(string)
  description = "List of CMR collection-concept-IDs to subscribe to. Note: Initial apply should have either no CCIDs listed or triggers disabled until the notification SNS topic is subscribed to"
}

variable "enable_triggers" {
  type        = bool
  description = "Whether to enable the SQS -> Lambda triggers. Note: Initial apply should have either no CCIDs listed or triggers disabled until the notification SNS topic is subscribed to"

  default = false
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

