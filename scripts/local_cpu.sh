#!/bin/bash
# Container CPU alongside Flink's busy%. They answer different questions:
#   busy% = fraction of time TASK THREADS are working (Flink metric)
#   CPU%  = actual processor usage (docker stats)
# Low busy + low CPU  = starved, the pipeline is waiting for input
# Low busy + high CPU = something else is eating the machine
# Without both, "we have spare capacity" is a guess.
docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}' 2>/dev/null \
  | awk -F'\t' '{printf "  %-34s cpu=%-8s mem=%s\n", $1, $2, $3}'
echo "  host cores: $(sysctl -n hw.ncpu 2>/dev/null)"
