#!/usr/bin/env bash
# Run the existing Java data generator against Confluent Cloud.
# Reads config/confluent.properties (written by terraform apply) and converts
# it to the generator's --kafka.props.* passthrough args. Extra args are
# forwarded, e.g.:
#   scripts/confluent_generator.sh --generator.trades.per.sec 100 --generator.prices.per.sec 200
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROPS="$REPO_ROOT/config/confluent.properties"
JAR="$REPO_ROOT/target/flink-demo.jar"

[[ -f "$PROPS" ]] || { echo "missing $PROPS — run terraform apply in infra-confluent/ first" >&2; exit 1; }
[[ -f "$JAR" ]] || { echo "missing $JAR — run: mvn clean package" >&2; exit 1; }

get() { grep "^$1=" "$PROPS" | head -1 | cut -d= -f2-; }

BOOTSTRAP="$(get bootstrap.servers)"
JAAS="$(get sasl.jaas.config)"

exec java -cp "$JAR" com.demo.flink.generator.DataGenerator \
  --kafka.bootstrap.servers "$BOOTSTRAP" \
  --kafka.props.security.protocol SASL_SSL \
  --kafka.props.sasl.mechanism PLAIN \
  --kafka.props.sasl.jaas.config "$JAAS" \
  "$@"
