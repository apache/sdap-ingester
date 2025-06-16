terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.34.0"
    }
    random = {
      source = "hashicorp/random"
    }
  }

  required_version = ">= 1.2.0"
}