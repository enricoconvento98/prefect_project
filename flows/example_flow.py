"""
Example Prefect flow demonstrating basic orchestration patterns
"""
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from typing import List
import time
import random


@task(retries=3, retry_delay_seconds=2)
def extract_data(source: str) -> List[dict]:
    """Extract data from a source"""
    logger = get_run_logger()
    logger.info(f"Extracting data from {source}")
    
    # Simulate data extraction
    time.sleep(random.uniform(1, 3))
    
    # Simulate occasional failures for retry demonstration
    if random.random() < 0.2:
        raise Exception(f"Failed to extract from {source}")
    
    data = [
        {"id": i, "source": source, "value": random.randint(1, 100)}
        for i in range(10)
    ]
    
    logger.info(f"Extracted {len(data)} records from {source}")
    return data


@task
def transform_data(data: List[dict]) -> List[dict]:
    """Transform the extracted data"""
    logger = get_run_logger()
    logger.info(f"Transforming {len(data)} records")
    
    # Simple transformation: add calculated field
    transformed = []
    for record in data:
        record["transformed_value"] = record["value"] * 2
        record["category"] = "high" if record["value"] > 50 else "low"
        transformed.append(record)
    
    time.sleep(1)  # Simulate processing time
    logger.info(f"Transformed {len(transformed)} records")
    return transformed


@task
def load_data(data: List[dict], destination: str) -> int:
    """Load data to destination"""
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} records to {destination}")
    
    # Simulate loading process
    time.sleep(2)
    
    logger.info(f"Successfully loaded {len(data)} records to {destination}")
    return len(data)


@task
def validate_data(record_count: int) -> bool:
    """Validate the loaded data"""
    logger = get_run_logger()
    
    if record_count > 0:
        logger.info(f"Data validation passed: {record_count} records processed")
        return True
    else:
        logger.error("Data validation failed: No records processed")
        return False


@flow(
    name="etl-pipeline",
    task_runner=ConcurrentTaskRunner(),
    description="A simple ETL pipeline demonstrating Prefect orchestration"
)
def etl_pipeline(sources: List[str] = None, destination: str = "data_warehouse"):
    """
    Main ETL pipeline flow
    
    Args:
        sources: List of data sources to extract from
        destination: Target destination for loading data
    """
    logger = get_run_logger()
    
    if sources is None:
        sources = ["database_a", "api_b", "file_c"]
    
    logger.info(f"Starting ETL pipeline for sources: {sources}")
    
    # Extract data from multiple sources concurrently
    raw_data_futures = []
    for source in sources:
        future = extract_data.submit(source)
        raw_data_futures.append(future)
    
    # Wait for all extractions to complete and combine data
    all_raw_data = []
    for future in raw_data_futures:
        data = future.result()
        all_raw_data.extend(data)
    
    logger.info(f"Total raw records extracted: {len(all_raw_data)}")
    
    # Transform the combined data
    transformed_data = transform_data(all_raw_data)
    
    # Load the transformed data
    record_count = load_data(transformed_data, destination)
    
    # Validate the results
    is_valid = validate_data(record_count)
    
    if is_valid:
        logger.info("ETL pipeline completed successfully!")
        return {"status": "success", "records_processed": record_count}
    else:
        logger.error("ETL pipeline failed validation!")
        return {"status": "failed", "records_processed": record_count}


@flow(name="data-quality-check")
def data_quality_flow():
    """A separate flow for data quality checks"""
    logger = get_run_logger()
    logger.info("Running data quality checks...")
    
    # Simulate quality checks
    time.sleep(2)
    
    quality_score = random.uniform(0.8, 1.0)
    logger.info(f"Data quality score: {quality_score:.2f}")
    
    return {"quality_score": quality_score, "passed": quality_score > 0.85}


if __name__ == "__main__":
    # Run the flow locally for testing
    result = etl_pipeline()
    print(f"Pipeline result: {result}")
    
    quality_result = data_quality_flow()
    print(f"Quality check result: {quality_result}")