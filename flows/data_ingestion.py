"""
Real-time data ingestion and processing flows
Simulates incoming data from various sources and processes them
"""
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.blocks.system import JSON
from typing import List, Dict, Any, Optional
import asyncio
import json
import random
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import uuid


class DataSource(Enum):
    """Enumeration of different data sources"""
    IOT_SENSORS = "iot_sensors"
    USER_EVENTS = "user_events"
    TRANSACTION_DATA = "transaction_data"
    LOG_DATA = "log_data"
    EXTERNAL_API = "external_api"


class DataQuality(Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    ACCEPTABLE = "acceptable"
    POOR = "poor"
    INVALID = "invalid"


@dataclass
class IncomingDataRecord:
    """Data record structure"""
    id: str
    source: str
    timestamp: str
    data: Dict[Any, Any]
    quality_score: float
    batch_id: str
    metadata: Dict[str, Any]


@task(retries=2, retry_delay_seconds=1)
def simulate_iot_sensors() -> List[Dict]:
    """Simulate IoT sensor data"""
    logger = get_run_logger()
    logger.info("Generating IoT sensor data...")
    
    sensors = ["temp_01", "humidity_02", "pressure_03", "vibration_04", "light_05"]
    records = []
    
    for sensor in sensors:
        # Simulate multiple readings per sensor
        for _ in range(random.randint(5, 15)):
            record = {
                "sensor_id": sensor,
                "reading": round(random.uniform(10, 100), 2),
                "unit": "celsius" if "temp" in sensor else "percent" if "humidity" in sensor else "bar" if "pressure" in sensor else "hz" if "vibration" in sensor else "lux",
                "location": f"building_{random.choice(['A', 'B', 'C'])}",
                "status": random.choice(["normal", "normal", "normal", "warning", "critical"]),
                "battery_level": random.randint(20, 100)
            }
            records.append(record)
    
    # Simulate network delays
    time.sleep(random.uniform(0.5, 2))
    logger.info(f"Generated {len(records)} IoT sensor readings")
    return records


@task(retries=2, retry_delay_seconds=1)
def simulate_user_events() -> List[Dict]:
    """Simulate user interaction events"""
    logger = get_run_logger()
    logger.info("Generating user event data...")
    
    events = ["login", "logout", "page_view", "click", "purchase", "search", "download"]
    users = [f"user_{i:04d}" for i in range(1, 101)]
    records = []
    
    for _ in range(random.randint(20, 50)):
        record = {
            "user_id": random.choice(users),
            "event_type": random.choice(events),
            "page": f"/page/{random.choice(['home', 'products', 'about', 'contact', 'checkout'])}",
            "session_id": str(uuid.uuid4())[:8],
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "user_agent": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            "duration": random.randint(1, 300),
            "value": round(random.uniform(0, 1000), 2) if random.random() > 0.7 else None
        }
        records.append(record)
    
    time.sleep(random.uniform(0.3, 1.5))
    logger.info(f"Generated {len(records)} user events")
    return records


@task(retries=2, retry_delay_seconds=1)
def simulate_transaction_data() -> List[Dict]:
    """Simulate financial transaction data"""
    logger = get_run_logger()
    logger.info("Generating transaction data...")
    
    records = []
    for _ in range(random.randint(10, 30)):
        record = {
            "transaction_id": str(uuid.uuid4()),
            "account_id": f"ACC{random.randint(100000, 999999)}",
            "amount": round(random.uniform(-5000, 5000), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
            "transaction_type": random.choice(["debit", "credit", "transfer", "fee"]),
            "merchant": random.choice(["Amazon", "Walmart", "Starbucks", "Shell", "ATM_WITHDRAWAL", None]),
            "category": random.choice(["shopping", "food", "fuel", "entertainment", "utilities", "other"]),
            "country": random.choice(["US", "UK", "DE", "FR", "JP"]),
            "risk_score": round(random.uniform(0, 1), 3)
        }
        records.append(record)
    
    time.sleep(random.uniform(0.8, 2.5))
    logger.info(f"Generated {len(records)} transactions")
    return records


@task(retries=2, retry_delay_seconds=1)
def simulate_log_data() -> List[Dict]:
    """Simulate application log data"""
    logger = get_run_logger()
    logger.info("Generating log data...")
    
    log_levels = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]
    services = ["auth-service", "payment-service", "user-service", "notification-service", "analytics-service"]
    records = []
    
    for _ in range(random.randint(15, 40)):
        level = random.choice(log_levels)
        record = {
            "service": random.choice(services),
            "level": level,
            "message": f"Sample log message for {level} level",
            "request_id": str(uuid.uuid4())[:12],
            "response_time": random.randint(10, 5000),
            "status_code": random.choice([200, 201, 400, 401, 403, 404, 500, 502, 503]),
            "memory_usage": random.randint(100, 2048),
            "cpu_usage": round(random.uniform(0, 100), 1)
        }
        records.append(record)
    
    time.sleep(random.uniform(0.4, 1.2))
    logger.info(f"Generated {len(records)} log entries")
    return records


@task
def validate_data_quality(records: List[Dict], source: str) -> tuple[List[Dict], DataQuality]:
    """Validate data quality and assign quality scores"""
    logger = get_run_logger()
    logger.info(f"Validating data quality for {len(records)} records from {source}")
    
    validated_records = []
    quality_scores = []
    
    for record in records:
        # Simulate data quality checks
        score = 1.0
        issues = []
        
        # Check for missing values
        missing_count = sum(1 for v in record.values() if v is None or v == "")
        if missing_count > 0:
            score -= missing_count * 0.1
            issues.append(f"missing_values: {missing_count}")
        
        # Check for data types and ranges
        if source == "iot_sensors" and "reading" in record:
            if not isinstance(record["reading"], (int, float)) or record["reading"] < 0:
                score -= 0.2
                issues.append("invalid_reading")
        
        # Check for required fields based on source
        required_fields = {
            "iot_sensors": ["sensor_id", "reading"],
            "user_events": ["user_id", "event_type"],
            "transaction_data": ["transaction_id", "amount"],
            "log_data": ["service", "level"]
        }
        
        if source in required_fields:
            missing_required = [field for field in required_fields[source] if field not in record or record[field] is None]
            if missing_required:
                score -= len(missing_required) * 0.3
                issues.append(f"missing_required: {missing_required}")
        
        # Clamp score between 0 and 1
        score = max(0.0, min(1.0, score))
        quality_scores.append(score)
        
        # Add quality metadata to record
        record["quality_score"] = score
        record["quality_issues"] = issues
        validated_records.append(record)
    
    # Determine overall quality
    avg_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
    if avg_score >= 0.9:
        overall_quality = DataQuality.EXCELLENT
    elif avg_score >= 0.8:
        overall_quality = DataQuality.GOOD
    elif avg_score >= 0.6:
        overall_quality = DataQuality.ACCEPTABLE
    elif avg_score >= 0.4:
        overall_quality = DataQuality.POOR
    else:
        overall_quality = DataQuality.INVALID
    
    logger.info(f"Data quality assessment: {overall_quality.value} (avg score: {avg_score:.3f})")
    return validated_records, overall_quality


@task(retries=3, retry_delay_seconds=2)
def process_validated_data(records: List[Dict], source: str, quality: DataQuality) -> Dict:
    """Process validated data based on quality and source"""
    logger = get_run_logger()
    
    if quality == DataQuality.INVALID:
        logger.error(f"Skipping processing for {source} due to invalid data quality")
        return {"status": "skipped", "reason": "invalid_quality", "records_processed": 0}
    
    logger.info(f"Processing {len(records)} records from {source} with quality {quality.value}")
    
    processed_records = []
    for record in records:
        processed_record = record.copy()
        
        # Add processing metadata
        processed_record["processed_at"] = datetime.now().isoformat()
        processed_record["processing_version"] = "1.0"
        processed_record["source_system"] = source
        
        # Apply source-specific processing
        if source == "iot_sensors":
            processed_record = process_iot_data(processed_record)
        elif source == "user_events":
            processed_record = process_user_event_data(processed_record)
        elif source == "transaction_data":
            processed_record = process_transaction_data(processed_record)
        elif source == "log_data":
            processed_record = process_log_data(processed_record)
        
        processed_records.append(processed_record)
    
    # Simulate processing time
    time.sleep(random.uniform(0.5, 2.0))
    
    result = {
        "status": "success",
        "source": source,
        "records_processed": len(processed_records),
        "quality": quality.value,
        "processing_time": time.time(),
        "processed_records": processed_records[:5]  # Sample of processed records
    }
    
    logger.info(f"Successfully processed {len(processed_records)} records from {source}")
    return result


def process_iot_data(record: Dict) -> Dict:
    """Apply IoT-specific processing"""
    # Add derived metrics
    if "reading" in record and "unit" in record:
        if record["unit"] == "celsius":
            record["reading_fahrenheit"] = (record["reading"] * 9/5) + 32
        
        # Add alerts based on thresholds
        if record.get("status") == "critical":
            record["alert_level"] = "high"
        elif record.get("status") == "warning":
            record["alert_level"] = "medium"
        else:
            record["alert_level"] = "low"
    
    return record


def process_user_event_data(record: Dict) -> Dict:
    """Apply user event-specific processing"""
    # Add session enrichment
    record["event_category"] = "engagement" if record.get("event_type") in ["page_view", "click"] else "transaction"
    
    # Add value scoring
    if record.get("value"):
        record["value_tier"] = "high" if record["value"] > 500 else "medium" if record["value"] > 100 else "low"
    
    return record


def process_transaction_data(record: Dict) -> Dict:
    """Apply transaction-specific processing"""
    # Add risk categorization
    risk_score = record.get("risk_score", 0)
    if risk_score > 0.8:
        record["risk_category"] = "high"
    elif risk_score > 0.5:
        record["risk_category"] = "medium"
    else:
        record["risk_category"] = "low"
    
    # Add amount categorization
    amount = abs(record.get("amount", 0))
    if amount > 1000:
        record["amount_category"] = "large"
    elif amount > 100:
        record["amount_category"] = "medium"
    else:
        record["amount_category"] = "small"
    
    return record


def process_log_data(record: Dict) -> Dict:
    """Apply log-specific processing"""
    # Add criticality scoring
    level = record.get("level", "INFO")
    record["criticality"] = {
        "FATAL": 5,
        "ERROR": 4,
        "WARN": 3,
        "INFO": 2,
        "DEBUG": 1
    }.get(level, 1)
    
    # Add performance flags
    response_time = record.get("response_time", 0)
    record["performance_flag"] = "slow" if response_time > 2000 else "normal"
    
    return record


@task
def aggregate_processing_results(results: List[Dict]) -> Dict:
    """Aggregate results from all data sources"""
    logger = get_run_logger()
    
    total_records = sum(r.get("records_processed", 0) for r in results)
    successful_sources = [r for r in results if r.get("status") == "success"]
    failed_sources = [r for r in results if r.get("status") != "success"]
    
    quality_distribution = {}
    for result in results:
        quality = result.get("quality", "unknown")
        quality_distribution[quality] = quality_distribution.get(quality, 0) + 1
    
    aggregated = {
        "total_records_processed": total_records,
        "successful_sources": len(successful_sources),
        "failed_sources": len(failed_sources),
        "quality_distribution": quality_distribution,
        "processing_timestamp": datetime.now().isoformat(),
        "source_details": results
    }
    
    logger.info(f"Aggregated results: {total_records} records from {len(successful_sources)} sources")
    return aggregated


@task
def store_processed_data(aggregated_results: Dict) -> bool:
    """Store processed data to persistent storage"""
    logger = get_run_logger()
    
    # In a real scenario, this would write to a database, data lake, etc.
    # For demo purposes, we'll simulate storage and log the summary
    
    storage_id = str(uuid.uuid4())[:8]
    storage_summary = {
        "storage_id": storage_id,
        "total_records": aggregated_results["total_records_processed"],
        "sources_count": aggregated_results["successful_sources"],
        "storage_timestamp": datetime.now().isoformat(),
        "quality_summary": aggregated_results["quality_distribution"]
    }
    
    # Simulate storage operation
    time.sleep(random.uniform(1, 3))
    
    logger.info(f"Stored data batch {storage_id}: {storage_summary}")
    return True


@flow(
    name="real-time-data-ingestion",
    task_runner=ConcurrentTaskRunner(),
    description="Ingests and processes real-time data from multiple sources"
)
def data_ingestion_flow(batch_id: str = None) -> Dict:
    """
    Main data ingestion flow that processes incoming data from multiple sources
    """
    logger = get_run_logger()
    
    if batch_id is None:
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    
    logger.info(f"Starting data ingestion for batch: {batch_id}")
    
    # Simulate data sources concurrently
    data_generation_tasks = {
        "iot_sensors": simulate_iot_sensors.submit(),
        "user_events": simulate_user_events.submit(),
        "transaction_data": simulate_transaction_data.submit(),
        "log_data": simulate_log_data.submit()
    }
    
    # Process each source as data becomes available
    processing_results = []
    for source_name, data_task in data_generation_tasks.items():
        try:
            # Get the generated data
            raw_data = data_task.result()
            
            # Validate data quality
            validated_data, quality = validate_data_quality(raw_data, source_name)
            
            # Process validated data
            result = process_validated_data(validated_data, source_name, quality)
            processing_results.append(result)
            
        except Exception as e:
            logger.error(f"Failed to process {source_name}: {str(e)}")
            processing_results.append({
                "status": "failed",
                "source": source_name,
                "error": str(e),
                "records_processed": 0
            })
    
    # Aggregate all results
    aggregated = aggregate_processing_results(processing_results)
    aggregated["batch_id"] = batch_id
    
    # Store processed data
    storage_success = store_processed_data(aggregated)
    
    final_result = {
        "batch_id": batch_id,
        "ingestion_status": "success" if storage_success else "partial_failure",
        "summary": aggregated,
        "completed_at": datetime.now().isoformat()
    }
    
    logger.info(f"Data ingestion completed for batch {batch_id}")
    return final_result


@flow(name="continuous-data-monitor")
def continuous_monitoring_flow(duration_minutes: int = 60):
    """
    Continuous monitoring flow that runs data ingestion at regular intervals
    """
    logger = get_run_logger()
    logger.info(f"Starting continuous monitoring for {duration_minutes} minutes")
    
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes)
    batch_count = 0
    
    while datetime.now() < end_time:
        batch_count += 1
        batch_id = f"monitor_batch_{batch_count}_{datetime.now().strftime('%H%M%S')}"
        
        try:
            result = data_ingestion_flow(batch_id)
            logger.info(f"Completed monitoring batch {batch_count}: {result['ingestion_status']}")
        except Exception as e:
            logger.error(f"Monitoring batch {batch_count} failed: {str(e)}")
        
        # Wait before next batch (simulate real-time intervals)
        time.sleep(30)  # 30 seconds between batches
    
    logger.info(f"Continuous monitoring completed. Processed {batch_count} batches.")
    return {"batches_processed": batch_count, "duration_minutes": duration_minutes}


if __name__ == "__main__":
    # Test the flows locally
    print("Testing data ingestion flow...")
    result = data_ingestion_flow()
    print(f"Ingestion result: {json.dumps(result, indent=2)}")
    
    print("\nTesting continuous monitoring (1 minute)...")
    monitor_result = continuous_monitoring_flow(duration_minutes=1)
    print(f"Monitoring result: {json.dumps(monitor_result, indent=2)}")