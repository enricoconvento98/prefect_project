"""
Example of a scheduled flow with deployments
"""
from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from datetime import datetime, timedelta
import requests


@task
def fetch_weather_data(city: str = "London") -> dict:
    """Fetch weather data for a city"""
    logger = get_run_logger()
    logger.info(f"Fetching weather data for {city}")
    
    # Using a free weather API (you'd need to replace with actual API key)
    # This is a mock response for demonstration
    mock_response = {
        "city": city,
        "temperature": 20.5,
        "humidity": 65,
        "description": "Partly cloudy",
        "timestamp": datetime.now().isoformat()
    }
    
    logger.info(f"Weather data retrieved for {city}: {mock_response['temperature']}°C")
    return mock_response


@task
def process_weather_data(weather_data: dict) -> dict:
    """Process and enrich weather data"""
    logger = get_run_logger()
    
    # Add some processing logic
    processed_data = weather_data.copy()
    processed_data["temperature_fahrenheit"] = (weather_data["temperature"] * 9/5) + 32
    processed_data["comfort_level"] = "comfortable" if 15 <= weather_data["temperature"] <= 25 else "uncomfortable"
    processed_data["processed_at"] = datetime.now().isoformat()
    
    logger.info(f"Processed weather data: {processed_data}")
    return processed_data


@task
def store_weather_data(processed_data: dict) -> bool:
    """Store processed weather data"""
    logger = get_run_logger()
    
    # Here you would typically store to a database
    # For demo purposes, we'll just log it
    logger.info(f"Storing weather data: {processed_data}")
    
    # Simulate storage operation
    return True


@flow(name="weather-pipeline")
def weather_monitoring_flow(cities: list = None):
    """
    A flow that monitors weather for multiple cities
    """
    logger = get_run_logger()
    
    if cities is None:
        cities = ["London", "New York", "Tokyo", "Sydney"]
    
    logger.info(f"Starting weather monitoring for cities: {cities}")
    
    results = []
    for city in cities:
        # Fetch weather data
        weather_data = fetch_weather_data(city)
        
        # Process the data
        processed_data = process_weather_data(weather_data)
        
        # Store the data
        stored = store_weather_data(processed_data)
        
        results.append({
            "city": city,
            "processed": stored,
            "data": processed_data
        })
    
    logger.info(f"Weather monitoring completed for {len(results)} cities")
    return results


# Create a deployment for scheduled execution
def create_weather_deployment():
    """Create a deployment with scheduling"""
    deployment = Deployment.build_from_flow(
        flow=weather_monitoring_flow,
        name="weather-monitoring-daily",
        schedule=CronSchedule(cron="0 8 * * *"),  # Run daily at 8 AM
        work_queue_name="default",
        parameters={"cities": ["London", "New York", "Tokyo", "Sydney", "Berlin"]},
        tags=["weather", "scheduled", "monitoring"]
    )
    return deployment


if __name__ == "__main__":
    # Test the flow locally
    result = weather_monitoring_flow()
    print(f"Weather monitoring result: {result}")
    
    # Create and apply deployment
    deployment = create_weather_deployment()
    deployment.apply()