# Confluent Cloud deployment — fully separate from infra/ (AWS).
# Auth, either of:
#   - ~/.confluent-creds with CONFLUENT_CLOUD_API_KEY=/CONFLUENT_CLOUD_API_SECRET= lines
#   - the same two names exported as env vars
# (a Cloud API key with OrganizationAdmin, from confluent.cloud → API keys).
terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.12"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

locals {
  creds_file = pathexpand("~/.confluent-creds")
  creds_raw  = fileexists(local.creds_file) ? file(local.creds_file) : ""

  cloud_api_key = length(regexall("CONFLUENT_CLOUD_API_KEY=(.*)", local.creds_raw)) > 0 ? (
    trimspace(regexall("CONFLUENT_CLOUD_API_KEY=(.*)", local.creds_raw)[0][0])
  ) : null
  cloud_api_secret = length(regexall("CONFLUENT_CLOUD_API_SECRET=(.*)", local.creds_raw)) > 0 ? (
    trimspace(regexall("CONFLUENT_CLOUD_API_SECRET=(.*)", local.creds_raw)[0][0])
  ) : null
}

provider "confluent" {
  cloud_api_key    = local.cloud_api_key    # null → falls back to env var
  cloud_api_secret = local.cloud_api_secret # null → falls back to env var
}
