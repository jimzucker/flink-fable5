#!/usr/bin/env bash
# Run LatencyProbe as a one-off Fargate task against the AWS stack.
#
# WHY THIS SCRIPT EXISTS
# Every probe run in phase 15 returned "NO RECORDS OBSERVED" for hours. The
# pipeline was fine the whole time. The cause was a hand-written network
# lookup in the ad-hoc command:
#
#     SG=$(aws ec2 describe-security-groups \
#          --filters Name=group-name,Values="*flink*" ...)
#
# which matched the MSK CLUSTER's security group instead of the client one,
# so the probe task had no path to Kafka. It failed silently: the consumer
# could not fetch metadata, and subscribe() turned that into an empty poll
# loop rather than an error. That silence was misread three times as
# pipeline failures (exactly-once broken, MV path broken, sinks not
# committing) — all wrong.
#
# The fix is to stop guessing: the generator ECS service already runs
# successfully against MSK, so its network configuration and task role are
# the known-good answer. Derive from it, never from a name wildcard.
#
# Usage:
#   scripts/run_probe.sh [topic] [duration_sec] [--verify-math]
#   scripts/run_probe.sh mv-by-account-ticker 180 --verify-math
set -euo pipefail

CLUSTER="${CLUSTER:-flink-fable5}"
SERVICE="${SERVICE:-flink-fable5-generator}"
TOPIC="${1:-mv-by-account-ticker}"
DURATION="${2:-120}"
VERIFY="${3:-}"

echo "probe: cluster=$CLUSTER topic=$TOPIC duration=${DURATION}s"

# --- network: taken from the running generator, not guessed ---
NET=$(aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" \
        --query 'services[0].networkConfiguration.awsvpcConfiguration' --output json)
SUBNETS=$(echo "$NET" | python3 -c "import json,sys;print(','.join(json.load(sys.stdin)['subnets']))")
SG=$(echo "$NET" | python3 -c "import json,sys;print(json.load(sys.stdin)['securityGroups'][0])")
echo "probe: subnets=$SUBNETS sg=$SG (from $SERVICE, known-good)"

# --- task definition: reuse the generator's role and Kafka auth args ---
python3 - "$TOPIC" "$DURATION" "$VERIFY" <<'PYEOF'
import json, subprocess, sys
topic, duration, verify = sys.argv[1], sys.argv[2], sys.argv[3]
td = json.loads(subprocess.run(
    ["aws", "ecs", "describe-task-definition", "--task-definition", "flink-fable5-generator",
     "--query", "taskDefinition"], capture_output=True, text=True).stdout)
c = td["containerDefinitions"][0]
cmd = c.get("command", [])
keep, i = [], 0
while i < len(cmd):  # carry only bootstrap + kafka.props.* (the IAM auth bits)
    if cmd[i] == "--kafka.bootstrap.servers" or cmd[i].startswith("--kafka.props."):
        keep += [cmd[i], cmd[i + 1]]; i += 2
    else:
        i += 1
c["entryPoint"] = ["java", "-cp", "/opt/app/flink-demo.jar",
                   "com.demo.flink.generator.LatencyProbe"]
c["command"] = keep + ["--probe.topic", topic, "--probe.duration.sec", duration]
if verify == "--verify-math":
    c["command"] += ["--probe.verify.math", "true"]
new = {k: td[k] for k in ["family", "taskRoleArn", "executionRoleArn", "networkMode",
                          "containerDefinitions", "requiresCompatibilities", "cpu", "memory"]
       if k in td}
new["family"] = "flink-fable5-probe"
subprocess.run(["aws", "ecs", "register-task-definition", "--cli-input-json", json.dumps(new),
                "--query", "taskDefinition.taskDefinitionArn", "--output", "text"],
               capture_output=True, text=True)
PYEOF

TASK=$(aws ecs run-task --cluster "$CLUSTER" --task-definition flink-fable5-probe \
         --launch-type FARGATE \
         --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=DISABLED}" \
         --query 'tasks[0].taskArn' --output text)
aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$TASK"
sleep 12

STREAM=$(aws logs describe-log-streams --log-group-name /ecs/flink-fable5-generator \
           --order-by LastEventTime --descending --max-items 1 \
           --query 'logStreams[0].logStreamName' --output text | head -1)
aws logs get-log-events --log-group-name /ecs/flink-fable5-generator \
    --log-stream-name "$STREAM" --limit 10 --query 'events[].message' --output text \
  | tr '\t' '\n' | grep -E "assigned|latency-probe|math-verify|Exception" || {
      echo "probe: no output found — check task $TASK"; exit 1; }
