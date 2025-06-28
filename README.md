# Prefect Docker Orchestration Project

A complete Prefect workflow orchestration setup with Docker, including server, worker, and example flows.

## 🏗️ Architecture

This project provides a fully containerized Prefect setup with:

- **Prefect Server**: Web UI and API server
- **PostgreSQL**: Database for Prefect metadata
- **Prefect Worker**: Executes flows using process-based execution
- **Example Flows**: Demonstrates ETL pipelines, scheduling, and task orchestration

## 📋 Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available for Docker
- Ports 4200 and 5432 available on your host machine

## 🚀 Quick Start

### Option 1: Using the Setup Script (Recommended)

```bash
# Make the setup script executable and run it
chmod +x setup.sh
./setup.sh
```

### Option 2: Using Make

```bash
# Run the complete setup
make setup

# Or step by step
make build
make up
make deploy
```

### Option 3: Manual Setup

```bash
# Start the services
docker compose up -d --build

# Wait for services to be ready (about 30 seconds)
# Create work pool
docker compose exec prefect-server prefect work-pool create default-pool --type process

# Deploy flows
docker compose exec prefect-server python deploy.py
```

## 📊 Accessing the Services

- **Prefect UI**: http://localhost:4200
- **PostgreSQL**: localhost:5432 (username: prefect, password: prefect)

## 🔧 Project Structure

```
.
├── docker compose.yml          # Multi-service Docker setup
├── Dockerfile.server          # Prefect server container
├── Dockerfile.worker          # Prefect worker container  
├── Dockerfile.agent           # Prefect agent container (alternative)
├── requirements.txt           # Python dependencies
├── setup.sh                   # Setup automation script
├── Makefile                   # Build and deployment commands
├── deploy.py                  # Flow deployment script
├── flows/
│   ├── example_flow.py        # ETL pipeline example
│   └── scheduled_flow.py      # Scheduled workflow example
└── README.md                  # This file
```

## 📦 Example Flows

### ETL Pipeline (`flows/example_flow.py`)
- Demonstrates data extraction from multiple sources
- Shows parallel task execution with ConcurrentTaskRunner
- Includes error handling and retries
- Implements data transformation and loading patterns

### Weather Monitoring (`flows/scheduled_flow.py`)
- Shows scheduled flow execution
- Demonstrates API integration patterns
- Includes data processing and storage workflows

## 🎯 Deployments

The project includes several pre-configured deployments:

1. **etl-pipeline-manual**: Manual trigger ETL pipeline
2. **etl-pipeline-scheduled**: ETL pipeline running every 6 hours
3. **data-quality-daily**: Daily data quality checks at 9 AM
4. **weather-monitoring-3h**: Weather monitoring every 3 hours

## 🛠️ Available Make Commands

```bash
make help          # Show all available commands
make setup         # Complete initial setup
make build         # Build Docker images
make up            # Start services
make down          # Stop services
make restart       # Restart services
make logs          # View logs
make logs-f        # Follow logs
make deploy        # Deploy flows
make clean         # Clean up Docker resources
make db-shell      # Access PostgreSQL shell
make server-shell  # Access Prefect server shell
make status        # Show service status
make run-etl       # Run ETL pipeline manually
make run-weather   # Run weather monitoring manually
make health-check  # Check service health
```

## 📈 Monitoring and Debugging

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f prefect-server
docker compose logs -f prefect-worker
docker compose logs -f postgres
```

### Check Service Health
```bash
# Using make
make health-check

# Manual check
curl http://localhost:4200/api/health
docker compose exec postgres pg_isready -U prefect
```

### Access Database
```bash
# Using make
make db-shell

# Manual access
docker compose exec postgres psql -U prefect -d prefect
```

## 🔄 Running Flows

### Via UI
1. Navigate to http://localhost:4200
2. Go to "Deployments" 
3. Click on