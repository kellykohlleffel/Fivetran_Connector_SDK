from fivetran_connector_sdk import Connector, Operations as op, Logging as log
from typing import Dict, Any
import requests
from datetime import datetime, timedelta
import time

def create_session():
    """Create session with retry logic"""
    session = requests.Session()
    return session

def schema(configuration: dict):
    """Define the table schemas for Fivetran"""
    return [
        {
            "table": "sleep",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "average_breath": "FLOAT",
                "average_heart_rate": "FLOAT",
                "average_hrv": "INT",
                "awake_time": "INT",
                "bedtime_end": "STRING",
                "bedtime_start": "STRING",
                "deep_sleep_duration": "INT",
                "efficiency": "INT",
                "latency": "INT",
                "light_sleep_duration": "INT",
                "low_battery_alert": "BOOLEAN",
                "lowest_heart_rate": "INT",
                "movement_30_sec": "STRING",
                "period": "INT",
                "rem_sleep_duration": "INT",
                "restless_periods": "INT",
                "sleep_phase_5_min": "STRING",
                "time_in_bed": "INT",
                "total_sleep_duration": "INT",
                "type": "STRING"
            }
        },
        {
            "table": "daily_activity",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "score": "INT",
                "active_calories": "INT",
                "average_met_minutes": "FLOAT",
                "equivalent_walking_distance": "INT",
                "high_activity_met_minutes": "INT",
                "high_activity_time": "INT",
                "inactivity_alerts": "INT",
                "low_activity_met_minutes": "INT",
                "low_activity_time": "INT",
                "medium_activity_met_minutes": "INT",
                "medium_activity_time": "INT",
                "meters_to_target": "INT",
                "non_wear_time": "INT",
                "resting_time": "INT",
                "steps": "INT",
                "target_calories": "INT",
                "target_meters": "INT",
                "total_calories": "INT"
            }
        },
        {
            "table": "daily_stress",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "stress_high": "INT",
                "recovery_high": "INT",
                "day_summary": "STRING"
            }
        }
    ]

def update(configuration: dict, state: dict) -> None:
    """Sync data incrementally from Oura API"""
    api_key = configuration['api_key']
    session = create_session()

    # Get last sync time or use default
    last_sync = state.get('last_sync_date')
    if last_sync:
        start_date = datetime.strptime(last_sync, '%Y-%m-%d').date()
    else:
        start_date = (datetime.now() - timedelta(days=30)).date()

    end_date = datetime.now().date()

    # Track request count for rate limiting
    request_count = 0

    # Headers for API requests
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    base_url = "https://api.ouraring.com/v2/usercollection"

    # Function to handle rate limiting
    def make_request(url, params=None):
        nonlocal request_count
        request_count += 1

        # Check rate limits
        if request_count >= 4900:  # Buffer for 5000 limit
            log.info("Approaching rate limit, sleeping for 5 minutes")
            time.sleep(300)  # Sleep for 5 minutes
            request_count = 0

        try:
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if response.status_code == 429:
                log.warning("Rate limit hit, sleeping for 5 minutes")
                time.sleep(300)
                return make_request(url, params)
            log.error(f"API request failed: {str(e)}")
            raise

    # Sync Sleep data
    params = {
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d')
    }

    sleep_data = make_request(f"{base_url}/sleep", params)
    for sleep in sleep_data.get('data', []):
        yield op.upsert('sleep', sleep)

    # Sync Activity data
    activity_data = make_request(f"{base_url}/daily_activity", params)
    for activity in activity_data.get('data', []):
        yield op.upsert('daily_activity', activity)

    # Sync Stress data
    stress_data = make_request(f"{base_url}/daily_stress", params)
    for stress in stress_data.get('data', []):
        yield op.upsert('daily_stress', stress)

    # Update state
    yield op.checkpoint({
        'last_sync_date': end_date.strftime('%Y-%m-%d')
    })

# Initialize the connector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()