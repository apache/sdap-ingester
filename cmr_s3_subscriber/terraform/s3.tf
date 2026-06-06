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

