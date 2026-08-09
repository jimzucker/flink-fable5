#!/usr/bin/env bash
# Run StreamValidator as a one-off Fargate task against the AWS stack.
#
# MSK sits in a private subnet, so correctness cannot be validated from a
# laptop. Network config and task role are derived from the RUNNING generator
# service -- the known-good answer -- never from a name wildcard, which once
# matched the MSK cluster's security group and produced hours of silent
# "no records" misdiagnosis.
set -euo pipefail
CLUSTER="${CLUSTER:-flink-fable5}"
SERVICE="${SERVICE:-flink-fable5-generator}"
export AWS_PAGER=""

NET=$(aws ecs describe-services --cluster "$CLUSTER" --services "$SERVICE" \
        --query 'services[0].networkConfiguration.awsvpcConfiguration' --output json)
SUBNETS=$(echo "$NET" | python3 -c "import json,sys;print(','.join(json.load(sys.stdin)['subnets']))")
SG=$(echo "$NET" | python3 -c "import json,sys;print(json.load(sys.stdin)['securityGroups'][0])")
echo "validate: subnets=$SUBNETS sg=$SG (from $SERVICE)"

python3 - <<'PYEOF'
import json, subprocess
td = json.loads(subprocess.run(
    ["aws","ecs","describe-task-definition","--task-definition","flink-fable5-generator",
     "--query","taskDefinition"], capture_output=True, text=True).stdout)
c = td["containerDefinitions"][0]
cmd = c.get("command", []); keep=[]; i=0
while i < len(cmd):                       # carry only bootstrap + IAM auth props
    if cmd[i] == "--kafka.bootstrap.servers" or cmd[i].startswith("--kafka.props."):
        keep += [cmd[i], cmd[i+1]]; i += 2
    else:
        i += 1
c["entryPoint"] = ["java","-cp","/opt/app/flink-demo.jar",
                  "com.demo.flink.generator.StreamValidator"]
c["command"] = keep + ["--validate.idle.ms","25000"]
new = {k: td[k] for k in ["family","taskRoleArn","executionRoleArn","networkMode",
                          "containerDefinitions","requiresCompatibilities","cpu","memory"] if k in td}
new["family"] = "flink-fable5-validate"
# The generator runs fine in 512MB; the validator does not -- it assigns every
# partition at once. Give the one-off validation task real memory (a few cents
# for a couple of minutes) rather than debugging OOMs that surface as silence.
new["cpu"] = "1024"
new["memory"] = "4096"
subprocess.run(["aws","ecs","register-task-definition","--cli-input-json",json.dumps(new),
                "--query","taskDefinition.taskDefinitionArn","--output","text"],
               capture_output=True, text=True)
PYEOF

TASK=$(aws ecs run-task --cluster "$CLUSTER" --task-definition flink-fable5-validate \
         --launch-type FARGATE \
         --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=DISABLED}" \
         --query 'tasks[0].taskArn' --output text)
aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$TASK"
# Report WHY the task ended before touching logs. A validator that dies on OOM
# or a pull error otherwise reads as "no output", which is indistinguishable
# from "ran fine but printed nothing" -- and the CloudWatch logs get destroyed
# with the infra, so the evidence is gone by the time anyone looks.
DESC=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$TASK" \
        --query 'tasks[0].[lastStatus,stopCode,stoppedReason,containers[0].exitCode]' --output text)
echo "validate: task ended -> $DESC"
sleep 20
ST=$(aws logs describe-log-streams --log-group-name /ecs/flink-fable5-generator \
       --order-by LastEventTime --descending --max-items 1 \
       --query 'logStreams[0].logStreamName' --output text | head -1)
# Retry: the validator's stream can lag behind the task reaching STOPPED.
for attempt in 1 2 3; do
  OUT=$(aws logs get-log-events --log-group-name /ecs/flink-fable5-generator \
          --log-stream-name "$ST" --limit 200 --query 'events[].message' \
          --output text 2>/dev/null | tr '\t' '\n')
  echo "$OUT" | grep -qE "streamed:|VALIDATION" && break
  echo "validate: no parseable output yet (attempt $attempt), re-reading in 20s"
  sleep 20
  ST=$(aws logs describe-log-streams --log-group-name /ecs/flink-fable5-generator \
         --order-by LastEventTime --descending --max-items 1 \
         --query 'logStreams[0].logStreamName' --output text | head -1)
done
echo "$OUT" | grep -E "streamed:|published:|\[PASS\]|\[FAIL\]|VALIDATION|lag tolerance" || {
      echo "validate: NO OUTPUT after 3 attempts — task $TASK"
      echo "--- last 40 raw log lines for diagnosis ---"
      echo "$OUT" | tail -40
      exit 1; }
