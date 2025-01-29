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
                "date": "STRING",
                "steps": "INT",
                "total_calories": "INT",
                "active_calories": "INT",
                "last_modified": "STRING"
            }
        },
        {
            "table": "daily_sleep",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "date": "STRING",
                "total_sleep_duration": "INT",
                "deep_sleep_duration": "INT",
                "light_sleep_duration": "INT",
                "rem_sleep_duration": "INT",
                "sleep_efficiency": "FLOAT",
                "last_modified": "STRING"
            }
        }
    ]


def make_api_request(session, api_key, endpoint, params=None):
    """Make API request with better error handling"""
    base_url = "https://api.ouraring.com/v2"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    try:
        url = f"{base_url}/{endpoint}"
        response = session.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()  # Raise an error for 4xx/5xx responses
        
        data = response.json()
        record_count = len(data.get('data', []))
        Logging.warning(f"Response from {endpoint} contains {record_count} records")

        if record_count > 0:
            sample_record = data['data'][0]
            Logging.warning(f"Sample record structure: {json.dumps(sample_record, indent=2)}")
        
        return data
    except rq.exceptions.HTTPError as e:
        Logging.warning(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        raise
    except Exception as e:
        Logging.warning(f"Unexpected error: {str(e)}")
        raise


def process_daily_activity(data):
    """Process daily activity data from the Oura API response."""
    processed_records = []
    
    for record in data.get('data', []):
        date_str = record.get('date') or record.get('timestamp') or record.get('day')

        if not date_str:
            Logging.warning(f"Skipping record with missing date: {record.get('id', 'unknown id')}")
            continue

        try:
            if 'T' in date_str:
                date_str = date_str.split('T')[0]  # Extract YYYY-MM-DD
            
            datetime.strptime(date_str, '%Y-%m-%d')  # Validate date format
        except ValueError:
            Logging.warning(f"Invalid date format: {date_str}")
            continue

        processed_record = {
            'id': str(record.get('id', '')),
            'date': date_str,
            'steps': int(record.get('steps', 0)),
            'total_calories': int(record.get('total_calories', 0)),
            'active_calories': int(record.get('active_calories', 0)),
            'last_modified': datetime.utcnow().isoformat()
        }
        processed_records.append(processed_record)

    return processed_records


def process_sleep_data(data):
    """Process daily sleep data from the Oura API response."""
    processed_records = []
    
    # Assumed base duration in seconds for 100% score (8 hours)
    BASE_SLEEP_DURATION = 8 * 60 * 60  # 8 hours in seconds
    
    if not data or 'data' not in data:
        Logging.warning("No data available to process")
        return processed_records
    
    for record in data.get('data', []):
        try:
            Logging.warning(f"Processing sleep record: {json.dumps(record, indent=2)}")
            
            date_str = record.get('day')
            if not date_str:
                Logging.warning(f"Skipping sleep record with missing date")
                continue
                
            contributors = record.get('contributors', {})
            
            # Convert percentage scores to durations based on the base duration
            total_sleep_score = contributors.get('total_sleep', 0) / 100.0
            deep_sleep_score = contributors.get('deep_sleep', 0) / 100.0
            rem_sleep_score = contributors.get('rem_sleep', 0) / 100.0
            
            # Calculate durations in seconds
            total_sleep_duration = int(BASE_SLEEP_DURATION * total_sleep_score)
            deep_sleep_duration = int(BASE_SLEEP_DURATION * 0.25 * deep_sleep_score)  # Assume ideal deep sleep is 25% of total
            rem_sleep_duration = int(BASE_SLEEP_DURATION * 0.25 * rem_sleep_score)    # Assume ideal REM is 25% of total
            light_sleep_duration = total_sleep_duration - (deep_sleep_duration + rem_sleep_duration)
            
            # Calculate sleep efficiency from the efficiency score
            sleep_efficiency = contributors.get('efficiency', 0) / 100.0
            
            processed_record = {
                'id': str(record.get('id', '')),
                'date': date_str,
                'total_sleep_duration': total_sleep_duration,
                'deep_sleep_duration': deep_sleep_duration,
                'light_sleep_duration': max(0, light_sleep_duration),  # Ensure non-negative
                'rem_sleep_duration': rem_sleep_duration,
                'sleep_efficiency': sleep_efficiency,
                'last_modified': datetime.utcnow().isoformat()
            }
            
            Logging.warning(f"Processed sleep record: {json.dumps(processed_record, indent=2)}")
            processed_records.append(processed_record)
            
        except Exception as e:
            Logging.warning(f"Error processing sleep record: {str(e)}")
            continue
    
    Logging.warning(f"Successfully processed {len(processed_records)} sleep records")
    return processed_records


def update(configuration: dict, state: dict):
    """Retrieve the most recent data from the Oura API."""
    session = create_retry_session()

    try:
        api_key = get_api_key(configuration)

        # Configure date range for October 2024
        start_date = "2024-10-01"
        end_date = "2024-10-31"

        Logging.warning(f"Fetching data from {start_date} to {end_date}")

        # Define the routes (daily_activity and daily_sleep)
        routes = [
            {
                'endpoint': 'usercollection/daily_activity',
                'table': 'daily_activity',
                'processor': process_daily_activity
            },
            {
                'endpoint': 'usercollection/daily_sleep',
                'table': 'daily_sleep',
                'processor': process_sleep_data
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
