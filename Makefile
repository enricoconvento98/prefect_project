# Prefect Docker Project Makefile

.PHONY: help build up down restart logs clean setup deploy

# Default target
help:
	@echo "Available commands:"
	@echo "  setup     - Initial setup (build, start services, create work pools, deploy flows)"
	@echo "  build     - Build Docker images"
	@echo "  up        - Start all services"
	@echo "  down      - Stop all services"
	@echo "  restart   - Restart all services"
	@echo "  logs      - View logs from all services"
	@echo "  logs-f    - Follow logs from all services"
	@echo "  deploy    - Deploy flows to Prefect server"
	@echo "  clean     - Clean up Docker resources"
	@echo "  db-shell  - Access PostgreSQL shell"
	@echo "  server-shell - Access Prefect server shell"
	@echo "  status    - Show service status"

# Initial setup
setup:
	@echo "🚀 Running initial setup..."
	@chmod +x setup.sh
	@./setup.sh

# Start services
up:
	@echo "🏗️  Building Docker images..."
	@docker compose up -d --build

# Stop services
down:
	@echo "🛑 Stopping services..."
	@docker compose down

# Restart services
restart:
	@echo "🔄 Restarting services..."
	@docker compose restart

# View logs
logs:
	@docker compose logs

# Follow logs
logs-f:
	@docker compose logs -f

# Deploy flows
deploy:
	@echo "📦 Deploying flows..."
	@docker compose exec prefect-server python /opt/prefect/deploy.py

# Clean up Docker resources
clean:
	@echo "🧹 Cleaning up Docker resources..."
	@docker compose down -v --remove-orphans
	@docker system prune -f

# Database shell
db-shell:
	@echo "🗄️  Accessing PostgreSQL shell..."
	@docker compose exec postgres psql -U prefect -d prefect

# Server shell
server-shell:
	@echo "🖥️  Accessing Prefect server shell..."
	@docker compose exec prefect-server bash

# Show status
status:
	@echo "📊 Service status:"
	@docker compose ps

# Development commands
dev-logs:
	@docker compose logs -f prefect-server prefect-worker

dev-restart:
	@docker compose restart prefect-server prefect-worker

test:
	@echo "Running tests..."
	@echo "TEST_PATH: $(TEST_PATH)"
	docker compose exec prefect-server python -m pytest -v --maxfail=1 $(or $(TEST_PATH),tests)
