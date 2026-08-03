variable "project" {
  description = "Name prefix for every Confluent resource"
  type        = string
  default     = "flink-fable5"
}

variable "region" {
  description = "Cloud region for the Kafka cluster and Flink compute pool"
  type        = string
  default     = "us-east-1"
}

variable "max_cfu" {
  description = "Compute-pool ceiling. CFUs autoscale 0..max; this is the cost cap and the scaling dial (the Confluent counterpart of flink_parallelism on AWS)."
  type        = number
  default     = 10
}
