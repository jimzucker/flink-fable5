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
