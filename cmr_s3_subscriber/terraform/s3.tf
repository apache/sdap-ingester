
resource "aws_s3_bucket" "provisioned_bucket" {
  bucket_prefix = var.bucket_name_prefix
  count         = var.existing_bucket == null ? 1 : 0
}


data "aws_s3_bucket" "existing_bucket" {
  bucket = var.existing_bucket
  count  = var.existing_bucket != null ? 1 : 0
}

locals {
  bucket = var.existing_bucket == null ? aws_s3_bucket.provisioned_bucket[0].id : data.aws_s3_bucket.existing_bucket[0].id
}

