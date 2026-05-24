.PHONY: help up down build logs ps clean setup

COMPOSE = docker compose
ENV_FILE = .env

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Initial setup — copy .env.example and prompt for NVIDIA key
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "⚠  Created .env — PLEASE EDIT and set NVIDIA_API_KEY before running 'make up'"; \
	else \
		echo "✓ .env already exists"; \
	fi

up: ## Start the full KalpanaOS stack
	$(COMPOSE) --env-file $(ENV_FILE) up -d
	@echo ""
	@echo "✓ KalpanaOS is starting up…"
	@echo ""
	@echo "  Web Dashboard  → http://localhost:3000"
	@echo "  Grafana        → http://localhost:3001"
	@echo "  Prometheus     → http://localhost:9090"
	@echo "  NATS Monitor   → http://localhost:8222"
	@echo ""
	@echo "  Default login: admin@kalpana.local / changeme123!"
	@echo "  (set via ADMIN_EMAIL / ADMIN_PASSWORD in .env)"

down: ## Stop all services
	$(COMPOSE) down

restart: ## Restart all services
	$(COMPOSE) restart

rebuild: ## Rebuild all Go service images and restart
	$(COMPOSE) build --no-cache sil col ssi aicp aaf ui
	$(COMPOSE) up -d

logs: ## Follow logs from all services
	$(COMPOSE) logs -f --tail=100

logs-%: ## Follow logs from a specific service (e.g. make logs-aicp)
	$(COMPOSE) logs -f --tail=100 $*

ps: ## Show service status
	$(COMPOSE) ps

clean: ## Remove all containers, volumes, and images
	$(COMPOSE) down -v --rmi local
	@echo "✓ All KalpanaOS data and images removed"

# ─── Go module tasks ───────────────────────────────────────────

tidy: ## Run go mod tidy on all services
	@for svc in sil col ssi aicp aaf; do \
		echo "→ Tidying services/$$svc"; \
		cd services/$$svc && go mod tidy; cd ../..; \
	done
	@echo "→ Tidying cli"; cd cli && go mod tidy; cd ..

build-cli: ## Build the kalpana CLI binary
	cd cli && go mod tidy && go build -o ../kalpana .
	@echo "✓ CLI built: ./kalpana"
	@echo "  Usage: ./kalpana status"
	@echo "  Usage: ./kalpana auth login --email admin@kalpana.local --password changeme123!"

# ─── Quick checks ──────────────────────────────────────────────

health: ## Check health of all services
	@echo "Checking service health…"
	@for port in 8001 8002 8003 8004 8005; do \
		name=$$([ $$port -eq 8001 ] && echo "SIL" || [ $$port -eq 8002 ] && echo "COL" || [ $$port -eq 8003 ] && echo "SSI" || [ $$port -eq 8004 ] && echo "AICP" || echo "AAF"); \
		result=$$(curl -sf http://localhost:$$port/health 2>/dev/null && echo "✓" || echo "✕"); \
		echo "  $$result $$name (port $$port)"; \
	done

status: ps health ## Show status overview
