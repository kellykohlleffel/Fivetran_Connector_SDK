import json
import time
from datetime import datetime, timedelta
import requests as rq
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging
from fivetran_connector_sdk import Operations as op


def create_retry_session():
    """Create a requests session with retry logic"""
    session = rq.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[408, 429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


def get_api_key(configuration):
    """Retrieve the API key from the configuration."""
    api_key = configuration.get('api_key')
    if not api_key:
        raise KeyError("Missing api_key in configuration")
    return str(api_key)


def schema(configuration: dict):
    """Define the table schemas for Fivetran."""
    return [
        {
            "table": "daily_activity",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "date": "NAIVE_DATE",
                "steps": "INT",
                "total_calories": "INT",
                "active_calories": "INT",
                "last_modified": "UTC_DATETIME"
            }
        },
        {
            "table": "heart_rate",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "timestamp": "UTC_DATETIME",
                "bpm": "INT",
                "source": "STRING",
                "last_modified": "UTC_DATETIME"
            }
        },
        {
            "table": "sleep",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "start_datetime": "UTC_DATETIME",
                "end_datetime": "UTC_DATETIME",
                "sleep_duration": "INT",
                "deep_sleep_duration": "INT",
                "rem_sleep_duration": "INT",
                "light_sleep_duration": "INT",
                "last_modified": "UTC_DATETIME"
            }
        },
        {
            "table": "ring_configuration",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "color": "STRING",
                "design": "STRING",
                "hardware_type": "STRING",
                "last_modified": "UTC_DATETIME"
            }
        }
    ]


def make_api_request(session, api_key, endpoint, params=None):
    """Make API request with error handling and logging"""
    base_url = "https://api.ouraring.com/v2"
    
    if params is None:
        params = {}
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    log_params = params.copy()
    Logging.warning(f"Making request to {endpoint} with params: {log_params}")

    try:
        url = f"{base_url}/{endpoint}"
        response = session.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except rq.exceptions.RequestException as e:
        Logging.warning(f"API request failed: {str(e)}")
        raise


def process_daily_activity(data):
    """Process daily activity data from the Oura API response."""
    processed_records = []
    
    for record in data.get('data', []):
        # Only process records that have a valid date
        date_str = record.get('date')
        if not date_str:
            Logging.warning(f"Skipping record with missing date: {record.get('id', 'unknown id')}")
            continue
            
        try:
            # Validate date format
            datetime.strptime(date_str, '%Y-%m-%d')
            
            processed_record = {
                'id': str(record.get('id', '')),
                'date': date_str,
                'steps': int(record.get('steps', 0)),
                'total_calories': int(record.get('total_calories', 0)),
                'active_calories': int(record.get('active_calories', 0)),
                'last_modified': datetime.utcnow().isoformat()
            }
            processed_records.append(processed_record)
        except ValueError as e:
            Logging.warning(f"Skipping record with invalid date format: {date_str}")
            continue
    
    return processed_records


def process_heart_rate(data):
    """Process heart rate data from the Oura API response."""
    processed_records = []
    
    for record in data.get('data', []):
        timestamp = record.get('timestamp')
        if not timestamp:
            Logging.warning(f"Skipping heart rate record with missing timestamp")
            continue
            
        try:
            # Validate timestamp format by parsing it
            datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            processed_record = {
                'id': str(record.get('id', '')),
                'timestamp': timestamp,
                'bpm': int(record.get('bpm', 0)),
                'source': str(record.get('source', '')),
                'last_modified': datetime.utcnow().isoformat()
            }
            processed_records.append(processed_record)
        except ValueError as e:
            Logging.warning(f"Skipping record with invalid timestamp format: {timestamp}")
            continue
    
    return processed_records


def process_sleep(data):
    """Process sleep data from the Oura API response."""
    processed_records = []
    
    for record in data.get('data', []):
        start_time = record.get('start_datetime')
        end_time = record.get('end_datetime')
        
        if not start_time or not end_time:
            Logging.warning(f"Skipping sleep record with missing timestamps")
            continue
            
        try:
            # Validate timestamp formats
            datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            processed_record = {
                'id': str(record.get('id', '')),
                'start_datetime': start_time,
                'end_datetime': end_time,
                'sleep_duration': int(record.get('duration', 0)),
                'deep_sleep_duration': int(record.get('deep_sleep_duration', 0)),
                'rem_sleep_duration': int(record.get('rem_sleep_duration', 0)),
                'light_sleep_duration': int(record.get('light_sleep_duration', 0)),
                'last_modified': datetime.utcnow().isoformat()
            }
            processed_records.append(processed_record)
        except ValueError as e:
            Logging.warning(f"Skipping record with invalid timestamp format: start={start_time}, end={end_time}")
            continue
    
    return processed_records


def update(configuration: dict, state: dict):
    """Retrieve the most recent data from the Oura API."""
    session = create_retry_session()
    
    try:
        api_key = get_api_key(configuration)
        
        # Configure date range
        start_date = "2024-10-01"
        end_date = "2024-10-30"
        
        routes = [
            {
                'endpoint': 'usercollection/daily_activity',
                'table': 'daily_activity',
                'processor': process_daily_activity
            },
            {
                'endpoint': 'usercollection/heartrate',
                'table': 'heart_rate',
                'processor': process_heart_rate
            },
            {
                'endpoint': 'usercollection/sleep',
                'table': 'sleep',
                'processor': process_sleep
            }
        ]

        for route in routes:
            Logging.warning(f"Starting sync for {route['table']}")
            
            params = {
                'start_date': start_date,
                'end_date': end_date
            }

            try:
                # Make API request
                data = make_api_request(session, api_key, route['endpoint'], params)
                processed_records = route['processor'](data)

                # Process and upsert records
                for record in processed_records:
                    yield op.upsert(
                        table=route['table'],
                        data=record
                    )
                
                Logging.warning(f"Processed {len(processed_records)} records for {route['table']}")

            except Exception as e:
                Logging.warning(f"Error processing {route['table']}: {str(e)}")
                raise

        # Ring configuration is handled separately as it doesn't need date filtering
        try:
            ring_data = make_api_request(session, api_key, 'usercollection/personal_info')
            if ring_data.get('ring'):
                ring_record = {
                    'id': str(ring_data['ring'].get('id', '')),
                    'color': str(ring_data['ring'].get('color', '')),
                    'design': str(ring_data['ring'].get('design', '')),
                    'hardware_type': str(ring_data['ring'].get('hardware_type', '')),
                    'last_modified': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                }
                yield op.upsert(
                    table='ring_configuration',
                    data=ring_record
                )

        except Exception as e:
            Logging.warning(f"Error processing ring configuration: {str(e)}")
            raise

        # Checkpoint after successful sync
        yield op.checkpoint({
            "last_sync_date": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        })

        Logging.warning("Sync complete")
        
    except Exception as e:
        Logging.warning(f"Error during sync: {str(e)}")
        raise


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    Logging.warning("Starting Oura Ring connector debug run...")
    connector.debug()
    Logging.warning("Debug run complete.")