
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
