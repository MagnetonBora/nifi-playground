# NiFi playground: NiFi (single or cluster) + Registry + Postgres + Kafka + monitoring + lab stubs.
# All stacks run as ONE compose project ($(PROJECT), via -p) and share the external
# Docker network "nifi-net"; data lives in named volumes (nifi-playground_*).
#
#   make help        list targets
#   make up          single NiFi + everything else
#   make up-cluster  3-node NiFi cluster + everything else

NETWORK := nifi-net
PROJECT := nifi-playground

# Stacks are started one file at a time inside the same project, so services of the other
# files look like "orphans" to compose: silence that warning (and never use --remove-orphans).
export COMPOSE_IGNORE_ORPHANS := True
COMPOSE := docker compose -p $(PROJECT)

SINGLE     := -f nifi/single/docker-compose-nifi-single.yml
CLUSTER    := -f nifi/cluster/docker-compose-nifi-cluster.yml
REGISTRY   := -f nifi/registry/docker-compose-nifi-registry.yml
POSTGRES   := -f postgres/docker-compose-postgres.yml
KAFKA      := -f kafka/docker-compose-kafka-single-node.yml
PROMETHEUS := -f monitoring/prometheus/docker-compose-prometheus.yml
GRAFANA    := -f monitoring/grafana/docker-compose-grafana.yml
LABS       := -f lab-services/docker-compose-lab-services.yml
SPLUNK     := -f splunk/docker-compose-splunk.yml

# Post-hook after `up`: remove finished one-shot init containers (label playground.role=init).
# Running containers are never touched; compose re-creates init jobs on the next `up`.
CLEAN_INIT = @docker container prune -f --filter label=playground.role=init >/dev/null

.DEFAULT_GOAL := help
.PHONY: help network up up-cluster down down-all \
        nifi cluster registry postgres kafka monitoring prometheus grafana splunk policies-nifi policies-cluster \
        nifi-down cluster-down registry-down postgres-down kafka-down monitoring-down prometheus-down grafana-down splunk-down \
        ps logs urls destroy clean-init \
        lab-services lab-services-down lab-setup import import-cluster lab03-measure lab4-insert lab05-data lab-final-load lab-splunk-count

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

network: ## Create the shared Docker network (idempotent)
	@docker network inspect $(NETWORK) >/dev/null 2>&1 || docker network create $(NETWORK)

# ---- whole environment -------------------------------------------------------

up: network registry postgres kafka monitoring lab-services nifi urls ## Start single NiFi + Registry, Postgres, Kafka, monitoring, lab stubs

up-cluster: network registry postgres kafka monitoring lab-services cluster urls ## Start NiFi cluster + Registry, Postgres, Kafka, monitoring, lab stubs

down: ## Stop everything (containers removed, data in volumes kept)
	-$(COMPOSE) $(SINGLE) down
	-$(COMPOSE) $(CLUSTER) down
	-$(COMPOSE) $(LABS) down
	-$(COMPOSE) $(SPLUNK) down
	-$(COMPOSE) $(GRAFANA) down
	-$(COMPOSE) $(PROMETHEUS) down
	-$(COMPOSE) $(KAFKA) down
	-$(COMPOSE) $(POSTGRES) down
	-$(COMPOSE) $(REGISTRY) down

down-all: down ## Alias for down

# ---- individual stacks -------------------------------------------------------

nifi: network ## Start single-node NiFi (https://localhost:8443/nifi) and grant policies
	$(COMPOSE) $(SINGLE) up -d
	$(CLEAN_INIT)
	@$(MAKE) --no-print-directory policies-nifi

cluster: network ## Start 3-node NiFi cluster (https://localhost:9443/nifi) and grant policies
	$(COMPOSE) $(CLUSTER) up -d
	$(CLEAN_INIT)
	@$(MAKE) --no-print-directory policies-cluster

policies-nifi: ## Wait for single NiFi, grant admin + prometheus policies (idempotent)
	$(COMPOSE) $(SINGLE) run --rm --no-deps nifi-policies

policies-cluster: ## Wait for the cluster, grant admin + nodes + prometheus policies (idempotent)
	$(COMPOSE) $(CLUSTER) run --rm --no-deps nifi-cluster-policies

registry: network ## Start NiFi Registry (https://localhost:18443/nifi-registry) + its LDAP (lldap)
	$(COMPOSE) $(REGISTRY) up -d
	$(CLEAN_INIT)

postgres: network ## Start PostgreSQL (localhost:5432) + pgAdmin (http://localhost:5050)
	$(COMPOSE) $(POSTGRES) up -d

kafka: network ## Start Kafka + Conduktor (localhost:9092, http://localhost:8080)
	$(COMPOSE) $(KAFKA) up -d

monitoring: prometheus grafana ## Start Prometheus + Grafana

prometheus: network ## Start Prometheus (scrapes Kafka + NiFi, http://localhost:9090) + kafka-exporter
	$(COMPOSE) $(PROMETHEUS) up -d
	$(CLEAN_INIT)

grafana: network ## Start Grafana (http://localhost:3000)
	$(COMPOSE) $(GRAFANA) up -d

splunk: network ## Start Splunk Enterprise trial (http://localhost:8000), not part of `up`; first start takes minutes
	$(COMPOSE) $(SPLUNK) up -d

nifi-down: ## Stop single-node NiFi
	$(COMPOSE) $(SINGLE) down

cluster-down: ## Stop NiFi cluster
	$(COMPOSE) $(CLUSTER) down

registry-down: ## Stop NiFi Registry + LDAP
	$(COMPOSE) $(REGISTRY) down

postgres-down: ## Stop PostgreSQL + pgAdmin
	$(COMPOSE) $(POSTGRES) down

kafka-down: ## Stop Kafka + Conduktor
	$(COMPOSE) $(KAFKA) down

monitoring-down: grafana-down prometheus-down ## Stop Prometheus + Grafana

prometheus-down: ## Stop Prometheus + kafka-exporter
	$(COMPOSE) $(PROMETHEUS) down

grafana-down: ## Stop Grafana
	$(COMPOSE) $(GRAFANA) down

splunk-down: ## Stop Splunk
	$(COMPOSE) $(SPLUNK) down

# ---- workshop labs (flow definitions in nifi/flows, see nifi/flows/README.md) ----

lab-services: network ## Start lab stubs: customers-api, splunk-sim, flaky
	$(COMPOSE) $(LABS) up -d

lab-services-down: ## Stop lab stubs
	$(COMPOSE) $(LABS) down

lab-setup: ## Prepare labs: Postgres tables, Kafka topics, data dirs (needs postgres + kafka; idempotent)
	docker exec -i nifi-postgres psql -q -U nifi -d nifi -v ON_ERROR_STOP=1 < postgres/init/01-lab4-customers.sql
	docker exec -i nifi-postgres psql -q -U nifi -d nifi -v ON_ERROR_STOP=1 < postgres/init/02-lab5-orders.sql
	@for t in orders.raw:1 orders.enriched:1 orders.dlq:1 logs.raw:6 logs.dlq:1; do \
		docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 --create --if-not-exists \
			--topic $${t%%:*} --partitions $${t##*:} --replication-factor 1 >/dev/null && echo "topic $${t%%:*} ok"; \
	done
	mkdir -p nifi/data/in nifi/data/out nifi/data/quarantine nifi/data/dlq nifi/data/ok

import: ## Import flow definitions into single NiFi: make import f="nifi/flows/lab2-*.json"
	@test -n "$(f)" || { echo 'usage: make import f=nifi/flows/<file>.json'; exit 1; }
	$(COMPOSE) $(SINGLE) run --rm --no-deps nifi-import $(addprefix /flows/,$(notdir $(wildcard $(f))))

import-cluster: ## Import flow definitions into the cluster: make import-cluster f=nifi/flows/<file>.json
	@test -n "$(f)" || { echo 'usage: make import-cluster f=nifi/flows/<file>.json'; exit 1; }
	$(COMPOSE) $(CLUSTER) run --rm --no-deps nifi-cluster-import $(addprefix /flows/,$(notdir $(wildcard $(f))))

lab03-measure: ## Lab 3: run split vs record variants, report to terminal and nifi/data/results/ (single NiFi)
	@mkdir -p nifi/data/results
	$(COMPOSE) $(SINGLE) run --rm --no-deps lab3-measure

lab4-insert: ## Lab 4: add n (default 10) new gold customers to table customers: make lab4-insert n=10
	docker exec nifi-postgres psql -q -U nifi -d nifi -v ON_ERROR_STOP=1 -c \
		"INSERT INTO customers (name, tier, country) SELECT 'New customer ' || g, 'gold', 'DE' FROM generate_series(1, $(or $(n),10)) AS g"
	@docker exec nifi-postgres psql -At -U nifi -d nifi -c "SELECT 'customers: ' || count(*) || ' rows, max id ' || max(id) FROM customers"

lab05-data: ## Lab 5: send 100 test orders (82 valid, 6 per error path) to Kafka topic orders.raw
	docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic orders.raw < lab-services/data/lab5-orders.ndjson

lab-final-load: ## Final lab: send n (default 100000) log events to logs.raw: make lab-final-load n=10000
	python3 lab-services/produce_logs.py $(or $(n),100000) | \
		docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw

lab-splunk-count: ## Lines received by the Splunk emulator (TCP and HEC)
	@docker exec splunk-sim sh -c 'touch /data/splunk_in.log /data/hec_in.log; wc -l /data/splunk_in.log /data/hec_in.log'

clean-init: ## Remove finished one-shot init containers (certs, registry-init)
	$(CLEAN_INIT)

# ---- inspection --------------------------------------------------------------

ps: ## Show all containers of the project
	@docker ps -a --filter label=com.docker.compose.project=$(PROJECT) --format 'table {{.Names}}\t{{.Label "com.docker.compose.service"}}\t{{.Status}}'

logs: ## Follow logs of one container: make logs s=nifi1
	@test -n "$(s)" || { echo "usage: make logs s=<container>  (see: make ps)"; exit 1; }
	docker logs -f --tail 200 $(s)

urls: ## Print service URLs
	@echo ""
	@echo "  NiFi (single)   https://localhost:8443/nifi        admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
	@echo "  NiFi (cluster)  https://localhost:9443/nifi  (:9444, :9445)"
	@echo "  NiFi Registry   https://localhost:18443/nifi-registry  admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB (LDAP)"
	@echo "  Registry users  http://localhost:17170 (lldap)         admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
	@echo "                  from NiFi: https://nifi-registry:18443 (client created by make policies-*)"
	@echo "  PostgreSQL      localhost:5432  nifi / nifi            (from NiFi: postgres:5432)"
	@echo "  pgAdmin         http://localhost:5050  admin@example.com / CHANGE_ME_TO_A_STRONG_PASSWORD"
	@echo "  Kafka           localhost:9092                         (from NiFi: kafka1:19092)"
	@echo "  Conduktor       http://localhost:8080"
	@echo "  Prometheus      http://localhost:9090"
	@echo "  Grafana         http://localhost:3000  admin / admin"
	@echo "  Splunk          http://localhost:8000  admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB  (make splunk; index=nifi)"
	@echo "                  from NiFi: tcp splunk:5514, HEC http://splunk:8088/services/collector/raw"
	@echo "  Lab stubs       customers-api:8000, splunk-sim:5514/8088, flaky:9099 (inside nifi-net)"
	@echo "  Lab 2 HTTP in   http://localhost:8081/events (ListenHTTP, single NiFi)"
	@echo "  TLS CA for the NiFi UI (optional, import into the browser): nifi/certs/ca.crt"
	@echo ""

# ---- danger zone -------------------------------------------------------------

destroy: ## Stop everything AND delete all volumes (all data is lost), asks for confirmation
	@printf "This deletes ALL data (flows, topics, databases, dashboards). Type 'yes' to continue: "; \
		read ans; [ "$$ans" = "yes" ] || { echo "Aborted."; exit 1; }
	-$(COMPOSE) $(SINGLE) down -v
	-$(COMPOSE) $(CLUSTER) down -v
	-$(COMPOSE) $(LABS) down -v
	-$(COMPOSE) $(SPLUNK) down -v
	-$(COMPOSE) $(GRAFANA) down -v
	-$(COMPOSE) $(PROMETHEUS) down -v
	-$(COMPOSE) $(KAFKA) down -v
	-$(COMPOSE) $(POSTGRES) down -v
	-$(COMPOSE) $(REGISTRY) down -v
	@echo "Certificates in nifi/certs are kept; delete the folder to issue a new CA."
