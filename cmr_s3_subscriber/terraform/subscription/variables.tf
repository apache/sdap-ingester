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


variable "ccid" {
  type = string
  description = "Collection concept ID for this subscription"
}

variable "config_file" {
  type = string
  description = "Script config file path"
}

variable "script_dir" {
  type = string
  description = "Path to directory containing CMR scripts and their venv"
}

variable "ddb_table" {
  type = string
  description = "DynamoDB table for CCID-options mappings"
}

variable "options" {
  type = object({
    shortname = string
    s3_path = optional(string)
    polygon = optional(string)
    trigger_on_revisions = optional(bool, true)
    delay = optional(number, 0)
    maap_config = optional(object({
      zarr_config_url = string
      variables = optional(string, "*")
    }))
  })
  description = "Collection options"
  nullable = true
}
