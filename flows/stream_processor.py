"""
Stream processing flow for handling high-frequency data streams
Simulates real-time event processing with windowing and aggregation
"""
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from typing import List, Dict, Any, Generator
import asyncio
import json
import random
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass
import uuid
import statistics


@dataclass
class StreamEvent:
    """Individual stream event"""
    event_id: str
    timestamp: datetime
    event_type: str
    source: str
    data: Dict[str, Any]
    partition_key: str


class StreamWindow:
    """Sliding window for stream aggregation"""
    
    def __init__(self, window_size_seconds: int = 60, slide_interval_seconds: int = 10):
        self.window_size = timedelta(seconds=window_size_seconds)
        self.slide_interval = timedelta(seconds=slide_interval_seconds)
        self.events = deque()
        self.last_slide_time = datetime.now()
    
    def add_event(self, event: StreamEvent):
        """Add event to window"""
        self.events.append(event)
        self._cleanup_old_events()
    
    def _cleanup_old_events(self):
        """Remove events outside the window"""
        cutoff_time = datetime.now() - self.window_size
        while self.events and self.events[0].timestamp < cutoff_time:
            self.events.popleft()
    
    def should_trigger_aggregation(self) -> bool:
        """Check if it's time to trigger aggregation"""
        return datetime.now() - self.last_slide_time >= self.slide_interval
    
    def get_events_for_aggregation(self) -> List[StreamEvent]:
        """Get current events in window for aggregation"""
        self._cleanup_old_events()
        self.last_slide_time = datetime.now()
        return list(self.events)


@task
def generate_high_frequency_stream(duration_seconds: int = 30, events_per_second: int = 10) -> List[StreamEvent]:
    """Generate high-frequency stream events"""
    logger = get_run_logger()
    logger.info(f"Generating high-frequency stream for {duration_seconds} seconds at {events_per_second} events/sec")
    
    events = []
    start_time = datetime.now()
    event_types = ["click", "view", "purchase", "search", "logout", "error", "alert"]
    sources = ["web_app", "mobile_app", "api", "sensor_network", "user_service"]
    
    for second in range(duration_seconds):
        for event_num in range(events_per_second):
            # Add some randomness to timing
            event_time = start_time + timedelta(seconds=second) + timedelta(milliseconds=random.randint(0, 999))
            
            event = StreamEvent(
                event_id=str(uuid.uuid4()),
                timestamp=event_time,
                event_type=random.choice(event_types),
                source=random.choice(sources),
                data={
                    "user_id": f"user_{random.randint(1000, 9999)}",
                    "session_id": str(uuid.uuid4())[:8],
                    "value": round(random.uniform(1, 1000), 2),
                    "category": random.choice(["A", "B", "C", "D"]),
                    "region": random.choice(["US", "EU", "ASIA", "SA"]),
                    "device_type": random.choice(["desktop", "mobile", "tablet"]),
                    "response_time": random.randint(10, 5000)
                },
                partition_key=f"partition_{random.randint(1, 4)}"
            )
            events.append(event)
    
    logger.info(f"Generated {len(events)} stream events")
    return events


@task
def partition_stream_events(events: List[StreamEvent]) -> Dict[str, List[StreamEvent]]:
    """Partition events by partition key for parallel processing"""
    logger = get_run_logger()
    
    partitioned = defaultdict(list)
    for event in events:
        partitioned[event.partition_key].append(event)
    
    logger.info(f"Partitioned {len(events)} events into {len(partitioned)} partitions")
    return dict(partitioned)


@task
def process_event_partition(partition_key: str, events: List[StreamEvent]) -> Dict[str, Any]:
    """Process events in a single partition"""
    logger = get_run_logger()
    logger.info(f"Processing partition {partition_key} with {len(events)} events")
    
    # Initialize window for this partition
    window = StreamWindow(window_size_seconds=60, slide_interval_seconds=10)
    
    # Process events through the window
    aggregation_results = []
    for event in events:
        window.add_event(event)
        
        # Check if we should trigger aggregation
        if window.should_trigger_aggregation():
            window_events = window.get_events_for_aggregation()
            if window_events:
                aggregation = aggregate_window_events(window_events, partition_key)
                aggregation_results.append(aggregation)
    
    # Final aggregation for remaining events
    final_events = window.get_events_for_aggregation()
    if final_events:
        final_aggregation = aggregate_window_events(final_events, partition_key)
        aggregation_results.append(final_aggregation)
    
    return {
        "partition_key": partition_key,
        "events_processed": len(events),
        "aggregations_generated": len(aggregation_results),
        "aggregations": aggregation_results
    }


def aggregate_window_events(events: List[StreamEvent], partition_key: str) -> Dict[str, Any]:
    """Aggregate events within a window"""
    if not events:
        return {}
    
    # Basic aggregations
    event_count = len(events)
    event_types = defaultdict(int)
    sources = defaultdict(int)
    categories = defaultdict(int)
    regions = defaultdict(int)
    values = []
    response_times = []
    
    for event in events:
        event_types[event.event_type] += 1
        sources[event.source] += 1
        
        if 'category' in event.data:
            categories[event.data['category']] += 1
        if 'region' in event.data:
            regions[event.data['region']] += 1
        if 'value' in event.data:
            values.append(event.data['value'])
        if 'response_time' in event.data:
            response_times.append(event.data['response_time'])
    
    # Calculate statistics
    aggregation = {
        "partition_key": partition_key,
        "window_start": min(e.timestamp for e in events).isoformat(),
        "window_end": max(e.timestamp for e in events).isoformat(),
        "event_count": event_count,
        "unique_users": len(set(e.data.get('user_id') for e in events if 'user_id' in e.data)),
        "unique_sessions": len(set(e.data.get('session_id') for e in events if 'session_id' in e.data)),
        "event_types": dict(event_types),
        "sources": dict(sources),
        "categories": dict(categories),
        "regions": dict(regions),
        "aggregated_at": datetime.now().isoformat()
    }
    
    # Value statistics
    if values:
        aggregation["value_stats"] = {
            "count": len(values),
            "sum": sum(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
            "std_dev": statistics.stdev(values) if len(values) > 1 else 0
        }
    
    # Response time statistics
    if response_times:
        aggregation["response_time_stats"] = {
            "count": len(response_times),
            "mean": statistics.mean(response_times),
            "median": statistics.median(response_times),
            "p95": sorted(response_times)[int(len(response_times) * 0.95)],
            "p99": sorted(response_times)[int(len(response_times) * 0.99)],
            "min": min(response_times),
            "max": max(response_times)
        }
    
    return aggregation


@task
def detect_anomalies(aggregations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Detect anomalies in aggregated data"""
    logger = get_run_logger()
    logger.info(f"Analyzing {len(aggregations)} aggregations for anomalies")
    
    anomalies = []
    
    for aggregation in aggregations:
        partition_key = aggregation.get("partition_key", "unknown")
        detected_anomalies = []
        
        # Check for high event count
        event_count = aggregation.get("event_count", 0)
        if event_count > 100:  # Threshold for high activity
            detected_anomalies.append({
                "type": "high_event_count",
                "value": event_count,
                "threshold": 100,
                "severity": "medium"
            })
        
        # Check for high error rates
        event_types = aggregation.get("event_types", {})
        error_count = event_types.get("error", 0)
        error_rate = error_count / event_count if event_count > 0 else 0
        if error_rate > 0.1:  # 10% error rate threshold
            detected_anomalies.append({
                "type": "high_error_rate",
                "value": error_rate,
                "threshold": 0.1,
                "severity": "high"
            })
        
        # Check for high response times
        response_stats = aggregation.get("response_time_stats", {})
        mean_response_time = response_stats.get("mean", 0)
        if mean_response_time > 2000:  # 2 second threshold
            detected_anomalies.append({
                "type": "high_response_time",
                "value": mean_response_time,
                "threshold": 2000,