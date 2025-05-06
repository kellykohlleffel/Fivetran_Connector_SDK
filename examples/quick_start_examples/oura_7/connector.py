from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests
import time
from datetime import datetime, timezone, timedelta
from dateutil import parser

def get_api_key(configuration):
    """Retrieve and validate the API key from configuration."""
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("Oura API key is missing from configuration")
        raise ValueError("API key is required")
    return api_key

def get_start_date(configuration, state):
    """Get the start date for data fetching, using configuration or state."""
    if state and state.get('last_sync_date'):
        # Use last sync date from state if available
        return state.get('last_sync_date')

    # Default to March 1, 2025 or use config override
    configured_start_date = configuration.get('start_date', '2025-03-01')
    try:
        # Validate the date format
        datetime.strptime(configured_start_date, '%Y-%m-%d')
        return configured_start_date
    except ValueError:
        log.warning(f"Invalid start_date format: {configured_start_date}. Using default: 2025-03-01")
        return '2025-03-01'

def make_api_request(session, url, params=None, max_retries=5):
    """Make API request with retry logic for rate limiting."""
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = session.get(url, params=params)

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                log.warning(f"Rate limited. Waiting for {retry_after} seconds")
                time.sleep(retry_after)
                retry_count += 1
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            retry_count += 1
            wait_time = 2 ** retry_count  # Exponential backoff

            if retry_count >= max_retries:
                log.severe(f"Failed after {max_retries} retries: {str(e)}")
                raise

            log.warning(f"Request failed. Retrying in {wait_time} seconds. Error: {str(e)}")
            time.sleep(wait_time)

    raise Exception("Maximum retries exceeded")

def fetch_daily_activity(session, base_url, start_date, end_date):
    """Fetch daily activity data for the given date range."""
    log.info(f"Fetching daily activity data from {start_date} to {end_date}")

    url = f"{base_url}/daily_activity"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }

    return make_api_request(session, url, params)

def fetch_daily_sleep(session, base_url, start_date, end_date):
    """Fetch daily sleep data for the given date range."""
    log.info(f"Fetching daily sleep data from {start_date} to {end_date}")

    url = f"{base_url}/daily_sleep"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }

    return make_api_request(session, url, params)

def safe_score(data, key):
    """Safely extract a score value and ensure it's a number."""
    try:
        value = data.get(key, 0)
        return float(value) if isinstance(value, (int, float)) else 0.0
    except Exception:
        return 0.0

def schema(configuration: dict):
    """Define the schema for Oura API data."""
    # Validate required configuration parameters
    try:
        get_api_key(configuration)
    except ValueError as e:
        log.severe(str(e))
        return []

    return [
        {
            "table": "daily_activity",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "class_5_min": "STRING",
                "score": "INT",
                "active_calories": "INT",
                "average_met_minutes": "FLOAT",
                "contributor_meet_daily_targets": "FLOAT",
                "contributor_move_every_hour": "FLOAT",
                "contributor_recovery_time": "FLOAT",
                "contributor_stay_active": "FLOAT",
                "contributor_training_frequency": "FLOAT",
                "contributor_training_volume": "FLOAT",
                "equivalent_walking_distance": "FLOAT",
                "high_activity_met_minutes": "FLOAT",
                "high_activity_time": "INT",
                "inactivity_alerts": "INT",
                "low_activity_met_minutes": "FLOAT",
                "low_activity_time": "INT",
                "medium_activity_met_minutes": "FLOAT",
                "medium_activity_time": "INT",
                "meters_to_target": "INT",
                "non_wear_time": "INT",
                "resting_time": "INT",
                "sedentary_met_minutes": "FLOAT",
                "sedentary_time": "INT",
                "steps": "INT",
                "target_calories": "INT",
                "target_meters": "INT",
                "total_calories": "INT",
                "timestamp": "STRING"
            }
        },
        {
            "table": "daily_sleep",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "average_breath": "FLOAT",
                "average_heart_rate": "FLOAT",
                "average_hrv": "FLOAT",
                "awake_time": "INT",
                "bedtime_end": "STRING",
                "bedtime_start": "STRING",
                "contributor_deep_sleep": "FLOAT",
                "contributor_efficiency": "FLOAT",
                "contributor_latency": "FLOAT", 
                "contributor_rem_sleep": "FLOAT",
                "contributor_restfulness": "FLOAT",
                "contributor_timing": "FLOAT",
                "contributor_total_sleep": "FLOAT",
                "deep_sleep_duration": "INT",
                "efficiency": "INT",
                "heart_rate_lowest": "FLOAT",
                "latency": "INT",
                "light_sleep_duration": "INT",
                "lowest_heart_rate_time_offset": "INT",
                "movement_30_sec": "STRING",
                "period": "INT",
                "rem_sleep_duration": "INT",
                "restless_periods": "INT",
                "score": "INT",
                "sleep_phase_5_min": "STRING",
                "sleep_score_delta": "INT",
                "time_in_bed": "INT",
                "total_sleep_duration": "INT",
                "timestamp": "STRING"
            }
        }
    ]

def update(configuration: dict, state: dict):
    """Extract data from the Oura API and yield operations."""
    try:
        # 1. Setup API connection
        api_key = get_api_key(configuration)
        base_url = "https://api.ouraring.com/v2/usercollection"

        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })

        # 2. Determine date range
        start_date = get_start_date(configuration, state)
        current_date = datetime.now().strftime('%Y-%m-%d')

        log.info(f"Starting Oura sync from {start_date} to {current_date}")

        # 3. Fetch daily activity data
        try:
            activity_response = fetch_daily_activity(session, base_url, start_date, current_date)
            activity_data = activity_response.get('data', [])

            log.info(f"Found {len(activity_data)} daily activity records")

            for record in activity_data:
                activity_id = record.get('id')
                day = record.get('day')

                if not activity_id or not day:
                    log.warning(f"Skipping activity record with missing id or day: {record}")
                    continue

                # Process contributors safely
                contributors = record.get('contributors', {})

                activity_record = {
                    "id": activity_id,
                    "day": day,
                    "class_5_min": record.get('class_5_min', ''),
                    "score": int(record.get('score', 0)),
                    "active_calories": int(record.get('active_calories', 0)),
                    "average_met_minutes": float(record.get('average_met_minutes', 0.0)),
                    "contributor_meet_daily_targets": safe_score(contributors, 'meet_daily_targets'),
                    "contributor_move_every_hour": safe_score(contributors, 'move_every_hour'),
                    "contributor_recovery_time": safe_score(contributors, 'recovery_time'),
                    "contributor_stay_active": safe_score(contributors, 'stay_active'),
                    "contributor_training_frequency": safe_score(contributors, 'training_frequency'),
                    "contributor_training_volume": safe_score(contributors, 'training_volume'),
                    "equivalent_walking_distance": float(record.get('equivalent_walking_distance', 0.0)),
                    "high_activity_met_minutes": float(record.get('high_activity_met_minutes', 0.0)),
                    "high_activity_time": int(record.get('high_activity_time', 0)),
                    "inactivity_alerts": int(record.get('inactivity_alerts', 0)),
                    "low_activity_met_minutes": float(record.get('low_activity_met_minutes', 0.0)),
                    "low_activity_time": int(record.get('low_activity_time', 0)),
                    "medium_activity_met_minutes": float(record.get('medium_activity_met_minutes', 0.0)),
                    "medium_activity_time": int(record.get('medium_activity_time', 0)),
                    "meters_to_target": int(record.get('meters_to_target', 0)),
                    "non_wear_time": int(record.get('non_wear_time', 0)),
                    "resting_time": int(record.get('resting_time', 0)),
                    "sedentary_met_minutes": float(record.get('sedentary_met_minutes', 0.0)),
                    "sedentary_time": int(record.get('sedentary_time', 0)),
                    "steps": int(record.get('steps', 0)),
                    "target_calories": int(record.get('target_calories', 0)),
                    "target_meters": int(record.get('target_meters', 0)),
                    "total_calories": int(record.get('total_calories', 0)),
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                }

                yield op.update("daily_activity", activity_record)

            # Checkpoint after processing all activity data
            if activity_data:
                log.info("Checkpoint after processing daily activity data")
                yield op.checkpoint({"last_sync_date": current_date})

        except Exception as e:
            log.severe(f"Error processing daily activity data: {str(e)}")

        # 4. Fetch daily sleep data
        try:
            sleep_response = fetch_daily_sleep(session, base_url, start_date, current_date)
            sleep_data = sleep_response.get('data', [])

            log.info(f"Found {len(sleep_data)} daily sleep records")

            for record in sleep_data:
                sleep_id = record.get('id')
                day = record.get('day')

                if not sleep_id or not day:
                    log.warning(f"Skipping sleep record with missing id or day: {record}")
                    continue

                # Process contributors safely
                contributors = record.get('contributors', {})

                sleep_record = {
                    "id": sleep_id,
                    "day": day,
                    "average_breath": float(record.get('average_breath', 0.0)),
                    "average_heart_rate": float(record.get('average_heart_rate', 0.0)),
                    "average_hrv": float(record.get('average_hrv', 0.0)),
                    "awake_time": int(record.get('awake_time', 0)),
                    "bedtime_end": record.get('bedtime_end', ''),
                    "bedtime_start": record.get('bedtime_start', ''),
                    "contributor_deep_sleep": safe_score(contributors, 'deep_sleep'),
                    "contributor_efficiency": safe_score(contributors, 'efficiency'),
                    "contributor_latency": safe_score(contributors, 'latency'),
                    "contributor_rem_sleep": safe_score(contributors, 'rem_sleep'),
                    "contributor_restfulness": safe_score(contributors, 'restfulness'),
                    "contributor_timing": safe_score(contributors, 'timing'),
                    "contributor_total_sleep": safe_score(contributors, 'total_sleep'),
                    "deep_sleep_duration": int(record.get('deep_sleep_duration', 0)),
                    "efficiency": int(record.get('efficiency', 0)),
                    "heart_rate_lowest": float(record.get('heart_rate_lowest', 0.0)),
                    "latency": int(record.get('latency', 0)),
                    "light_sleep_duration": int(record.get('light_sleep_duration', 0)),
                    "lowest_heart_rate_time_offset": int(record.get('lowest_heart_rate_time_offset', 0)),
                    "movement_30_sec": record.get('movement_30_sec', ''),
                    "period": int(record.get('period', 0)),
                    "rem_sleep_duration": int(record.get('rem_sleep_duration', 0)),
                    "restless_periods": int(record.get('restless_periods', 0)),
                    "score": int(record.get('score', 0)),
                    "sleep_phase_5_min": record.get('sleep_phase_5_min', ''),
                    "sleep_score_delta": int(record.get('sleep_score_delta', 0)),
                    "time_in_bed": int(record.get('time_in_bed', 0)),
                    "total_sleep_duration": int(record.get('total_sleep_duration', 0)),
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                }

                yield op.update("daily_sleep", sleep_record)

            # Final checkpoint after processing all data
            log.info("Final checkpoint after processing all data")
            yield op.checkpoint({"last_sync_date": current_date})

        except Exception as e:
            log.severe(f"Error processing daily sleep data: {str(e)}")

    except Exception as e:
        log.severe(f"Unexpected error in update function: {str(e)}")

# Create Connector instance
connector = Connector(update=update, schema=schema)

# Entry point for local debugging
if __name__ == "__main__":
    connector.debug()