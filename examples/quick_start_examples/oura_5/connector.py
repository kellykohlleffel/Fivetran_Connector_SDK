import time
import requests
from datetime import datetime, timezone, timedelta
from dateutil import parser
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

def get_api_key(configuration):
    """Retrieve the API key from the configuration."""
    api_key = configuration.get('api_key')
    if not api_key:
        raise KeyError("Missing api_key in configuration")
    return str(api_key)

def safe_score(contributors, key):
    """Safely extract a score value and ensure it's a number"""
    try:
        value = contributors.get(key, 0)
        return float(value) if isinstance(value, (int, float)) else 0.0
    except Exception:
        return 0.0

def schema(configuration: dict):
    """Define the table schema for Fivetran"""
    # Validate required configuration parameters
    try:
        get_api_key(configuration)
    except KeyError as e:
        log.severe(str(e))
        return []

    return [
        {
            "table": "daily_activity",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "active_calories": "INT",
                "average_met_minutes": "FLOAT",
                "contributors_meet_daily_targets": "FLOAT",
                "contributors_move_every_hour": "FLOAT", 
                "contributors_recovery_time": "FLOAT",
                "contributors_stay_active": "FLOAT",
                "contributors_training_frequency": "FLOAT",
                "contributors_training_volume": "FLOAT",
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
                "last_modified": "STRING"
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
                "contributors_deep_sleep": "FLOAT",
                "contributors_efficiency": "FLOAT",
                "contributors_latency": "FLOAT",
                "contributors_rem_sleep": "FLOAT",
                "contributors_restfulness": "FLOAT",
                "contributors_timing": "FLOAT",
                "contributors_total_sleep": "FLOAT",
                "day_id": "STRING",
                "deep_sleep_duration": "INT",
                "efficiency": "INT",
                "heart_rate_lowest": "FLOAT",
                "heart_rate_average": "FLOAT",
                "hrv_average": "FLOAT", 
                "latency": "INT",
                "light_sleep_duration": "INT",
                "low_battery_alert": "BOOLEAN",
                "lowest_heart_rate": "INT",
                "readiness_score_delta": "FLOAT",
                "rem_sleep_duration": "INT",
                "restless_periods": "INT",
                "sleep_phase_5_min": "STRING",
                "sleep_score_delta": "FLOAT",
                "time_in_bed": "INT",
                "total_sleep_duration": "INT",
                "last_modified": "STRING"
            }
        }
    ]

def update(configuration: dict, state: dict):
    """Extract data from the source and yield operations"""
    # 1. VALIDATE REQUIRED CONFIGURATION PARAMETERS
    try:
        api_key = get_api_key(configuration)
    except KeyError as e:
        log.severe(str(e))
        return

    # 2. SETUP API CLIENT WITH CONFIGURATION
    base_url = configuration.get('base_url', 'https://api.ouraring.com/v2')
    page_size = int(configuration.get('page_size', '100'))
    headers = {"Authorization": f"Bearer {api_key}"}
    session = requests.Session()
    session.headers.update(headers)

    # 3. GET LAST SYNC STATE OR USE DEFAULT START DATE (March 1, 2025)
    last_sync_timestamp = state.get('last_sync_timestamp')
    if last_sync_timestamp:
        start_date = parser.parse(last_sync_timestamp).date()
    else:
        # Default to March 1, 2025 as per requirements
        start_date = datetime(2025, 3, 1).date()

    # End date is today
    end_date = datetime.now(timezone.utc).date()
    current_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4. FETCH AND PROCESS DAILY ACTIVITY DATA
    try:
        log.info(f"Fetching daily activity data from {start_date} to {end_date}")

        # Format dates for API request
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Initialize pagination
        next_token = None
        has_more = True
        page_count = 0

        while has_more:
            try:
                # Build request params
                params = {
                    "start_date": start_date_str,
                    "end_date": end_date_str,
                    "page_size": page_size
                }

                if next_token:
                    params["next_token"] = next_token

                # Make API request
                response = session.get(f"{base_url}/usercollection/daily_activity", params=params)
                response.raise_for_status()
                data = response.json()

                # Process records
                for record in data.get('data', []):
                    activity_id = f"{record.get('day')}"  # Use day as ID

                    # Extract contributors safely
                    contributors = record.get('contributors', {})

                    activity_record = {
                        "id": activity_id,
                        "day": record.get('day'),
                        "active_calories": record.get('active_calories', 0),
                        "average_met_minutes": record.get('average_met_minutes', 0.0),
                        "contributors_meet_daily_targets": safe_score(contributors, 'meet_daily_targets'),
                        "contributors_move_every_hour": safe_score(contributors, 'move_every_hour'),
                        "contributors_recovery_time": safe_score(contributors, 'recovery_time'),
                        "contributors_stay_active": safe_score(contributors, 'stay_active'),
                        "contributors_training_frequency": safe_score(contributors, 'training_frequency'),
                        "contributors_training_volume": safe_score(contributors, 'training_volume'),
                        "equivalent_walking_distance": record.get('equivalent_walking_distance', 0.0),
                        "high_activity_met_minutes": record.get('high_activity_met_minutes', 0.0),
                        "high_activity_time": record.get('high_activity_time', 0),
                        "inactivity_alerts": record.get('inactivity_alerts', 0),
                        "low_activity_met_minutes": record.get('low_activity_met_minutes', 0.0),
                        "low_activity_time": record.get('low_activity_time', 0),
                        "medium_activity_met_minutes": record.get('medium_activity_met_minutes', 0.0),
                        "medium_activity_time": record.get('medium_activity_time', 0),
                        "meters_to_target": record.get('meters_to_target', 0),
                        "non_wear_time": record.get('non_wear_time', 0),
                        "resting_time": record.get('resting_time', 0),
                        "sedentary_met_minutes": record.get('sedentary_met_minutes', 0.0),
                        "sedentary_time": record.get('sedentary_time', 0),
                        "steps": record.get('steps', 0),
                        "target_calories": record.get('target_calories', 0),
                        "target_meters": record.get('target_meters', 0),
                        "total_calories": record.get('total_calories', 0),
                        "last_modified": current_timestamp
                    }

                    # Yield update operation
                    yield op.update("daily_activity", activity_record)

                # Check pagination
                next_token = data.get('next_token')
                has_more = bool(next_token)

                page_count += 1

                # Checkpoint every 5 pages
                if page_count % 5 == 0:
                    log.info(f"Checkpointing after processing {page_count} pages of daily activity data")
                    yield op.checkpoint({"last_sync_timestamp": current_timestamp})

                # Add small delay to avoid hitting rate limits
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                if e.response and e.response.status_code == 429:
                    # Rate limiting - implement backoff
                    retry_after = int(e.response.headers.get('Retry-After', 60))
                    log.warning(f"Rate limited. Waiting for {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                else:
                    # Handle other request errors
                    log.severe(f"Error fetching daily activity data: {str(e)}")
                    break

        # 5. FETCH AND PROCESS DAILY SLEEP DATA
        log.info(f"Fetching daily sleep data from {start_date} to {end_date}")

        # Reset pagination for sleep data
        next_token = None
        has_more = True
        page_count = 0

        while has_more:
            try:
                # Build request params
                params = {
                    "start_date": start_date_str,
                    "end_date": end_date_str,
                    "page_size": page_size
                }

                if next_token:
                    params["next_token"] = next_token

                # Make API request
                response = session.get(f"{base_url}/usercollection/daily_sleep", params=params)
                response.raise_for_status()
                data = response.json()

                # Process records
                for record in data.get('data', []):
                    sleep_id = f"{record.get('day')}"  # Use day as ID

                    # Extract contributors safely
                    contributors = record.get('contributors', {})

                    sleep_record = {
                        "id": sleep_id,
                        "day": record.get('day'),
                        "average_breath": record.get('average_breath', 0.0),
                        "average_heart_rate": record.get('average_heart_rate', 0.0),
                        "average_hrv": record.get('average_hrv', 0.0),
                        "awake_time": record.get('awake_time', 0),
                        "bedtime_end": record.get('bedtime_end', ''),
                        "bedtime_start": record.get('bedtime_start', ''),
                        "contributors_deep_sleep": safe_score(contributors, 'deep_sleep'),
                        "contributors_efficiency": safe_score(contributors, 'efficiency'),
                        "contributors_latency": safe_score(contributors, 'latency'),
                        "contributors_rem_sleep": safe_score(contributors, 'rem_sleep'),
                        "contributors_restfulness": safe_score(contributors, 'restfulness'),
                        "contributors_timing": safe_score(contributors, 'timing'),
                        "contributors_total_sleep": safe_score(contributors, 'total_sleep'),
                        "day_id": record.get('day_id', ''),
                        "deep_sleep_duration": record.get('deep_sleep_duration', 0),
                        "efficiency": record.get('efficiency', 0),
                        "heart_rate_lowest": record.get('heart_rate_lowest', 0.0),
                        "heart_rate_average": record.get('heart_rate_average', 0.0),
                        "hrv_average": record.get('hrv_average', 0.0),
                        "latency": record.get('latency', 0),
                        "light_sleep_duration": record.get('light_sleep_duration', 0),
                        "low_battery_alert": record.get('low_battery_alert', False),
                        "lowest_heart_rate": record.get('lowest_heart_rate', 0),
                        "readiness_score_delta": record.get('readiness_score_delta', 0.0),
                        "rem_sleep_duration": record.get('rem_sleep_duration', 0),
                        "restless_periods": record.get('restless_periods', 0),
                        "sleep_phase_5_min": str(record.get('sleep_phase_5_min', '')),
                        "sleep_score_delta": record.get('sleep_score_delta', 0.0),
                        "time_in_bed": record.get('time_in_bed', 0),
                        "total_sleep_duration": record.get('total_sleep_duration', 0),
                        "last_modified": current_timestamp
                    }

                    # Yield update operation
                    yield op.update("daily_sleep", sleep_record)

                # Check pagination
                next_token = data.get('next_token')
                has_more = bool(next_token)

                page_count += 1

                # Checkpoint every 5 pages
                if page_count % 5 == 0:
                    log.info(f"Checkpointing after processing {page_count} pages of daily sleep data")
                    yield op.checkpoint({"last_sync_timestamp": current_timestamp})

                # Add small delay to avoid hitting rate limits
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                if e.response and e.response.status_code == 429:
                    # Rate limiting - implement backoff
                    retry_after = int(e.response.headers.get('Retry-After', 60))
                    log.warning(f"Rate limited. Waiting for {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                else:
                    # Handle other request errors
                    log.severe(f"Error fetching daily sleep data: {str(e)}")
                    break

        # Final checkpoint
        yield op.checkpoint({"last_sync_timestamp": current_timestamp})

    except Exception as e:
        log.severe(f"Unexpected error: {str(e)}")

# Create Connector instance
connector = Connector(update=update, schema=schema)

# Main entry point for local debugging
if __name__ == "__main__":
    connector