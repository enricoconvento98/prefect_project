"""
Deployment script for Prefect flows
"""
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule
from prefect.filesystems import LocalFileSystem
from flows.example_flow import etl_pipeline, data_quality_flow
from flows.scheduled_flow import weather_monitoring_flow
import asyncio


async def create_deployments():
    """Create and apply all deployments"""
    
    # Create file system block for storing flow code
    fs_block = LocalFileSystem(basepath="/opt/prefect/flows")
    await fs_block.save("local-flows", overwrite=True)
    
    # ETL Pipeline Deployment - Manual trigger
    etl_deployment = Deployment.build_from_flow(
        flow=etl_pipeline,
        name="etl-pipeline-manual",
        work_queue_name="default",
        storage=fs_block,
        parameters={
            "sources": ["database_a", "api_b", "file_c"],
            "destination": "data_warehouse"
        },
        tags=["etl", "manual", "data-pipeline"]
    )
    
    # ETL Pipeline Deployment - Scheduled every 6 hours
    etl_scheduled_deployment = Deployment.build_from_flow(
        flow=etl_pipeline,
        name="etl-pipeline-scheduled",
        schedule=CronSchedule(cron="0 */6 * * *"),  # Every 6 hours
        work_queue_name="default",
        storage=fs_block,
        parameters={
            "sources": ["database_a", "api_b", "file_c", "external_api"],
            "destination": "data_warehouse"
        },
        tags=["etl", "scheduled", "data-pipeline"]
    )
    
    # Data Quality Check Deployment - Daily
    quality_deployment = Deployment.build_from_flow(
        flow=data_quality_flow,
        name="data-quality-daily",
        schedule=CronSchedule(cron="0 9 * * *"),  # Daily at 9 AM
        work_queue_name="default",
        storage=fs_block,
        tags=["quality", "scheduled", "monitoring"]
    )
    
    # Weather Monitoring Deployment - Every 3 hours
    weather_deployment = Deployment.build_from_flow(
        flow=weather_monitoring_flow,
        name="weather-monitoring-3h",
        schedule=IntervalSchedule(interval=10800),  # 3 hours in seconds
        work_queue_name="default",
        storage=fs_block,
        parameters={
            "cities": ["London", "New York", "Tokyo", "Sydney", "Berlin", "Paris"]
        },
        tags=["weather", "scheduled", "monitoring"]
    )
    
    # Apply all deployments
    deployments = [
        etl_deployment,
        etl_scheduled_deployment,
        quality_deployment,
        weather_deployment
    ]
    
    deployment_ids = []
    for deployment in deployments:
        deployment_id = await deployment.apply()
        deployment_ids.append(deployment_id)
        print(f"✅ Applied deployment: {deployment.name} (ID: {deployment_id})")
    
    print(f"\n🎉 Successfully created {len(deployment_ids)} deployments!")
    return deployment_ids


if __name__ == "__main__":
    asyncio.run(create_deployments())