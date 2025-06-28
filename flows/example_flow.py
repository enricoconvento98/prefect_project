"""
Example Prefect flow demonstrating basic orchestration patterns
"""
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from typing import List
import time
import random
from modules.database import Database


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


def _load_data_to_postgres(data: List[dict], table_name: str) -> int:
    """Core logic to load data to a PostgreSQL database using the Database module."""
    if not data:
        return 0

    try:
        with Database() as db:
            # Create table if it doesn't exist
            columns = list(data[0].keys())
            type_mapping = {
                int: "INTEGER",
                float: "REAL",
                str: "TEXT",
                bool: "BOOLEAN",
            }
            column_defs = [f'{col} {type_mapping.get(type(data[0][col]), "TEXT")}' for col in columns]
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)});"
            db.execute_query(create_table_sql)

            # Prepare data for insertion
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
            values = [tuple(d.values()) for d in data]
            db.execute_batch(insert_query, values)

            return len(data)

    except Exception as e:
        raise  # Re-raise the exception to be caught by the Prefect task

@task
def load_data(data: List[dict], destination: str) -> int:
    """Prefect task to load data to a PostgreSQL database."""
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} records to PostgreSQL table {destination}")
    try:
        record_count = _load_data_to_postgres(data, destination)
        logger.info(f"Successfully loaded {record_count} records to {destination}")
        return record_count
    except Exception as e:
        logger.error(f"Failed to load data to PostgreSQL: {e}")
        raise


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
        if data:
            all_raw_data.extend(data)

    if not all_raw_data:
        logger.info("No data was extracted. Skipping transformation and loading.")
        return {"status": "success", "records_processed": 0}
        
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
    # You will need to set the following environment variables for this to work:
    # POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    result = etl_pipeline(destination="my_test_table")
    print(f"Pipeline result: {result}")
    
    quality_result = data_quality_flow()
    print(f"Quality check result: {quality_result}")
