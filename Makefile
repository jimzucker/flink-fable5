.PHONY: build up down logs positions status clean test validate

test:
	mvn test

# Correctness validation against the running stack (pauses generator briefly)
validate:
	python3 scripts/validate_live.py

build:
	mvn -q -DskipTests package

up: build
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f generator

# Tail the position output topic
positions:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic position-by-account-ticker \
		--property print.key=true --from-beginning

# Tail any topic: make tail TOPIC=mv-by-ticker
TOPIC ?= mv-by-ticker
tail:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC) \
		--property print.key=true --from-beginning

status:
	curl -s http://localhost:8081/jobs/overview | python3 -m json.tool

clean: down
	rm -rf target data

# --- AWS (see docs/AWS_RUNBOOK.md) ---
REGION ?= us-east-1
ECR_REPO = $(shell cd infra && terraform output -raw generator_ecr_repo 2>/dev/null)

aws-push-generator: build
	aws ecr get-login-password --region $(REGION) | docker login --username AWS --password-stdin $(ECR_REPO)
	# --platform linux/amd64 is REQUIRED: building on Apple Silicon otherwise
	# produces an arm64 manifest and Fargate fails the pull with
	# "image Manifest does not contain descriptor matching platform 'linux/amd64'",
	# which surfaces only as an ECS placement error minutes later.
	docker build --platform linux/amd64 -f docker/generator.Dockerfile -t $(ECR_REPO):latest .
	docker push $(ECR_REPO):latest

aws-deploy: build
	cd infra && terraform apply

aws-destroy:
	cd infra && terraform destroy
