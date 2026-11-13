SHELL := /bin/bash
TF    := terraform -chdir=terraform
DC    := docker compose
KAFKA := $(DC) exec -T kafka /opt/kafka/bin

.DEFAULT_GOAL := help
.PHONY: ui help infra env sa-key up up-live down nuke logs ps topics alerts dlq tail-raw trigger reconcile test lint destroy

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- cloud (Terraform)
APIS := cloudresourcemanager.googleapis.com serviceusage.googleapis.com iam.googleapis.com \
        bigquery.googleapis.com storage.googleapis.com billingbudgets.googleapis.com

infra: ## enable APIs, then terraform init + apply (GCS, BigQuery, IAM, budget)
	@# Terraform reads the project via Cloud Resource Manager before it can enable anything,
	@# so on a brand-new project the APIs must be switched on first.
	gcloud services enable $(APIS) --project "$$(sed -n 's/^project_id *= *"\(.*\)"/\1/p' terraform/terraform.tfvars)"
	$(TF) init -upgrade
	$(TF) apply

env: ## Write GCP settings from terraform outputs into .env
	@test -f .env || cp .env.example .env
	@$(TF) output -raw env_file_snippet | while IFS='=' read -r k v; do \
	  [ -n "$$k" ] && { sed -i.bak "/^$$k=/d" .env && echo "$$k=$$v" >> .env; }; done; rm -f .env.bak
	@echo ".env updated"; grep -E '^WIKIPULSE_(GCP_PROJECT|BUCKET|BQ_LOCATION)=' .env

sa-key: ## Create a key for the pipeline service account into ./secrets/sa.json (git-ignored)
	@mkdir -p secrets
	gcloud iam service-accounts keys create secrets/sa.json \
	  --iam-account "$$($(TF) output -raw service_account_email)"
	@chmod 600 secrets/sa.json

destroy: ## Tear down all cloud resources
	$(TF) destroy

# ---------------------------------------------------------------- local runtime
# The source is written into .env (not just the shell), so a later
# `docker compose up -d producer` can't silently switch modes.
set-source = @sed -i.bak 's/^WIKIPULSE_SOURCE=.*/WIKIPULSE_SOURCE=$(1)/' .env && rm -f .env.bak

up: ## Start everything with the synthetic source (offline demo)
	@test -f .env || cp .env.example .env
	$(call set-source,synthetic)
	$(DC) up -d --build
	@echo "Airflow: http://localhost:8080   Spark UI: http://localhost:4040"

up-live: ## Start everything on the live Wikimedia stream
	@test -f .env || cp .env.example .env
	$(call set-source,wikimedia)
	$(DC) up -d --build

ui: ## Also start Kafka UI on :8081
	$(DC) --profile ui up -d kafka-ui

down: ## Stop containers (keeps volumes/data)
	$(DC) down

nuke: ## Stop and delete all local data (Kafka, lake, checkpoints, Airflow DB)
	$(DC) down -v

logs: ## Follow logs (S=service to filter)
	$(DC) logs -f --tail=100 $(S)

ps: ## Container status
	$(DC) ps

topics: ## Topic list with partitions/offsets
	$(KAFKA)/kafka-topics.sh --bootstrap-server kafka:9092 --describe

alerts: ## Live edit-storm alerts
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic wiki.alerts \
	  --from-beginning --property print.key=true --property key.separator=" | "

dlq: ## Dead-lettered records with reasons
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic wiki.recentchange.dlq \
	  --from-beginning --max-messages 20

tail-raw: ## Peek at the raw topic
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic wiki.recentchange.raw --max-messages 5

trigger: ## Process one hour now: make trigger HOUR=2026-11-12T05
	$(DC) exec airflow airflow dags trigger wikipulse_hourly -c '{"hour": "$(HOUR)"}'

reconcile: ## Reconcile a day: make reconcile DAY=2026-11-11
	$(DC) exec airflow airflow dags trigger wikipulse_reconcile -c '{"day": "$(DAY)"}'

# ---------------------------------------------------------------- quality
test: ## Run unit, Spark, contract and Terraform tests (needs Java 17+)
	python -m pytest -q tests

lint: ## Lint Python, check Terraform formatting
	ruff check .
	$(TF) fmt -check -recursive
