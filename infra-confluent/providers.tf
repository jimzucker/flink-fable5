# Confluent Cloud deployment — fully separate from infra/ (AWS).
# Auth: export CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET
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

provider "confluent" {}
