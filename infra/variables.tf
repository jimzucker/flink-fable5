variable "project" {
  description = "Name prefix for every resource (matches the git repo)"
  type        = string
  default     = "flink-fable5"
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

variable "topics_partitions" {
  description = "Partition count for topics (created or grown by the generator). Must be >= the highest parallelism under test: a Flink source cannot read a topic with more parallelism than it has partitions, so subtasks past the partition count sit idle. 48 covers the P=40 scaling rung with headroom. This is a BILLED dimension on MSK ($0.0015/partition-hr) and free on Confluent's eCKU model."
  type        = number
  default     = 48
}

variable "topics_recreate" {
  # ONE-TIME BOOTSTRAP ONLY. With more than one generator task, every task races
  # to delete and recreate all topics at once and MSK Serverless answers with
  # ThrottlingQuotaExceededException: the losers die at startup having logged
  # nothing past SLF4J init, ECS restarts them forever, and the seed silently
  # comes from a single survivor. Seen twice in one session, the second time
  # because the fix lived only in a scratchpad command line and did not survive
  # writing a new deploy script. It lives here now.
  description = "Delete and recreate topics on generator start. Set true ONCE to establish a clean partition layout with desired-count 1, then back to false. Leaving it true with multiple generator tasks throttles MSK and silently produces a fraction of the intended backlog."
  type        = bool
  default     = false
}

variable "kafka_extra_props" {
  description = "Extra kafka.props.* passthrough entries for the Flink app (e.g. sink batching: linger.ms, batch.size, compression.type)"
  type        = map(string)

  # transaction.timeout.ms is REQUIRED, not optional tuning, whenever the sink
  # runs exactly-once. Flink's producer default is 1 HOUR; MSK's broker maximum
  # (transaction.max.timeout.ms) is 15 MINUTES. Leave it unset and every sink
  # committer fails at InitProducerId with "The transaction timeout is larger
  # than the maximum value allowed by the broker", the job restart-loops, and
  # it reads nothing — which shows up as 100% busy with zero records, not as an
  # obvious error. It lived on the command line before, so it silently vanished
  # the first time a deploy was scripted. Defaulting it here is the fix.
  default = {
    "transaction.timeout.ms" = "540000"
  }
}

variable "flink_extra_props" {
  description = "Extra raw app properties for the Flink app (e.g. emit.interval.ms for conflated output emission)"
  type        = map(string)
  default     = {}
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

variable "snapshots_enabled" {
  description = "MSF snapshots. Keep true normally; set false while debugging a crash-looping app so config updates are not blocked by a failed snapshot."
  type        = bool
  default     = true
}
