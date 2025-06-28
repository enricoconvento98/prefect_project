"""
Updated deployment script for Prefect flows using modern API
"""
from prefect import serve
from datetime import timedelta
from flows.example_flow import etl_pipeline, data_quality_flow
from flows.scheduled_flow import weather_monitoring_flow


def create_deployments():
    """Create and serve all deployments using modern Prefect API"""
    
    # Create deployments using flow.to_deployment()
    deployments = []
    
    # # ETL Pipeline Deployment - Manual trigger
    # etl_manual = etl_pipeline.to_deployment(
    #     name="etl-pipeline-manual",
    #     parameters={
    #         "sources": ["database_a", "api_b", "file_c"],
    #         "destination": "data_warehouse"
    #     },
    #     tags=["etl", "manual", "data-pipeline"],
    #     description="ETL pipeline for manual execution"
    # )
    # deployments.append(etl_manual)
    
    # ETL Pipeline Deployment - Scheduled every 6 hours
    etl_scheduled = etl_pipeline.to_deployment(
        name="etl-pipeline-scheduled",
        cron="0 */6 * * *",  # Every 6 hours - use cron string directly
        parameters={
            "sources": ["database_a", "api_b", "file_c", "external_api"],
            "destination": "data_warehouse"
        },
        tags=["etl", "scheduled", "data-pipeline"],
        description="ETL pipeline scheduled every 6 hours"
    )
    deployments.append(etl_scheduled)
    
    # # Data Quality Check Deployment - Daily
    # quality_daily = data_quality_flow.to_deployment(
    #     name="data-quality-daily",
    #     cron="0 9 * * *",  # Daily at 9 AM - use cron string directly
    #     tags=["quality", "scheduled", "monitoring"],
    #     description="Daily data quality checks at 9 AM"
    # )
    # deployments.append(quality_daily)
    
    # # Weather Monitoring Deployment - Every 3 hours
    # weather_monitoring = weather_monitoring_flow.to_deployment(
    #     name="weather-monitoring-3h",
    #     interval=10800,  # 3 hours in seconds - use interval directly
    #     parameters={
    #         "cities": ["London", "New York", "Tokyo", "Sydney", "Berlin", "Paris"]
    #     },
    #     tags=["weather", "scheduled", "monitoring"],
    #     description="Weather monitoring every 3 hours for major cities"
    # )
    # deployments.append(weather_monitoring)
    
    print(f"🚀 Created {len(deployments)} deployments:")
    for deployment in deployments:
        print(f"  - {deployment.name}")
    
    # Serve all deployments
    print("\n🎯 Starting deployment server...")
    serve(*deployments)


if __name__ == "__main__":
    import sys
    
    create_deployments()