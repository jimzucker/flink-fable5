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
  description = <<-EOT
    Extra raw app properties for the Flink app.

    CONFLATE THE OUTPUT, NEVER THE INPUT. The code defaults already do this and
    they are correct -- do not override them without reading this:

      sql.price.conflate.ms      default 0     input windowing OFF
      mv.emit.interval.ms        default 1000  output rate limit ON
      position.emit.interval.ms  default 500   output rate limit ON

    Rate-limiting the OUTPUT publishes the newest value less often, so the
    published value is always current. Windowing the INPUT (a tumbling window
    over prices) discards the newest tick before it is used, so the published
    value can NEVER be current.

    Measured locally, 100 symbols @ 2,000 prices/s, identical 440k input:
      conflate=250 + emit=0     55,389 published, p50 2,929ms stale, 0% exact
      conflate=0   + emit=1000  18,386 published, 0ms stale,      100% exact
    The output-conflated config wins on BOTH axes -- fewer writes and exact
    values. There is no trade-off to tune here.

    The Phase 20 benchmark scripts set conflate=250 AND emit=0 -- backwards on
    both counts -- so every AWS SQL figure was measured on the dominated config.
  EOT
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
  default = 0
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

variable "generator_qty_override" {
  description = "Correctness runs: fixed trade quantity (1 makes position = deduped trade count); -1 = realistic random"
  type        = number
  default     = -1
}

variable "generator_price_per_symbol" {
  description = "Correctness runs: symbol i priced at $(i+1) rising 1 cent per tick, so a stale price read is detectable"
  type        = bool
  default     = false
}

variable "flink_log_level" {
  description = <<-EOT
    MSF application log level. INFO on a 40-subtask job across a dozen restarts
    ingested 8.25 GB of CloudWatch Logs in a single day -- one day consumed the
    entire 5 GB MONTHLY free tier and cost $2.36. WARN keeps failures and
    restarts visible while dropping per-subtask INFO chatter.
    Raise to INFO deliberately when debugging a specific job, not by default.
  EOT
  type    = string
  default = "WARN"
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

variable "generator_distribution" {
  description = "Volume distribution across symbols: uniform | zipf. Real tape is Pareto -- a small head carries most messages. Uniform makes key starvation the dominant effect and hides production skew."
  type        = string
  default     = "zipf"
}

variable "generator_zipf_alpha" {
  description = "Zipf shape. 1.0 gives roughly top-10 ~54%, top-100 ~72% of messages across a 3,000-symbol universe."
  type        = number
  default     = 1.0
}

variable "generator_ipo_share" {
  description = "Fraction of price ticks forced onto a single hot listing, on top of the baseline curve. 0.30 models an IPO / squeeze day."
  type        = number
  default     = 0.30
}

variable "generator_ipo_ticker" {
  description = "Index of the hot listing within the ticker universe."
  type        = number
  default     = 2999
}

variable "generator_price_key_mode" {
  description = "Kafka record key for prices: symbol | salted | adaptive. Keying by bare symbol puts a hot name in ONE partition, capping producers AND consumers. adaptive salts only symbols above a share threshold, so quiet names keep per-symbol ordering."
  type        = string
  default     = "adaptive"
}
