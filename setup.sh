#!/bin/bash

# Prefect Docker Setup Script
echo "🚀 Setting up Prefect with Docker..."

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p flows
mkdir -p data
mkdir -p logs

# Set permissions
chmod +x setup.sh

# Build and start services
echo "🐳 Building and starting Docker services..."
docker-compose up -d --build

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check if Prefect server is ready
echo "🔍 Checking Prefect server status..."
while ! curl -f http://localhost:4200/api/health > /dev/null 2>&1; do
    echo "Waiting for Prefect server to be ready..."
    sleep 5
done

echo "✅ Prefect server is ready!"

# Set Prefect API URL for local CLI
export PREFECT_API_URL="http://localhost:4200/api"

# Create work pool
echo "🏊 Creating work pool..."
docker-compose exec prefect-server prefect work-pool create default-pool --type process

# Deploy flows
echo "📦 Deploying flows..."
docker-compose exec prefect-server python /opt/prefect/deploy.py

echo "🎉 Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Visit http://localhost:4200 to access the Prefect UI"
echo "2. View your deployments and flows in the dashboard"
echo "3. Trigger flows manually or wait for scheduled runs"
echo ""
echo "🔧 Useful commands:"
echo "  View logs: docker-compose logs -f [service-name]"
echo "  Stop services: docker-compose down"
echo "  Restart services: docker-compose restart"
echo "  Access database: docker-compose exec postgres psql -U prefect -d prefect"