variable "project" {
  description = "Name prefix for every resource"
  type        = string
  default     = "flink-demo"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

# --- Scaling: config only, no rebuild ---
variable "flink_parallelism" {
  description = "Job parallelism (rescale by changing this and applying)"
  type        = number
  default     = 2
}

variable "flink_parallelism_per_kpu" {
  type    = number
  default = 1
}

# --- Generator knobs (perf cases are tfvars-only changes) ---
variable "generator_trades_per_sec" {
  type    = number
  default = 10
}

variable "generator_prices_per_sec" {
  type    = number
  default = 20
}

variable "generator_accounts" {
  type    = number
  default = 5
}

variable "generator_tickers" {
  type    = number
  default = 10
}

variable "generator_seed" {
  type    = number
  default = 42
}

variable "generator_duplicate_ratio" {
  type    = number
  default = 0.05
}

variable "generator_price_cents_override" {
  description = "Perf Case 2: set to a huge value (cents) to pin all prices; -1 = normal walk"
  type        = number
  default     = -1
}

variable "jar_path" {
  description = "Path to the shaded job jar (make build first)"
  type        = string
  default     = "../target/flink-demo.jar"
}

variable "generator_cpu" {
  description = "Fargate CPU units for the generator (256 = 0.25 vCPU)"
  type        = number
  default     = 256
}

variable "generator_memory" {
  type    = number
  default = 512
}
