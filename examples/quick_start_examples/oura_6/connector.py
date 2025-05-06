from fivetran_connector_sdk import Connector, Operations as op, Logging as log
import requests
from datetime import datetime, timezone, timedelta
from dateutil import parser

def schema(configuration: dict):
    """Define the table schema for Fivetran"""
    # Validate required configuration parameters
    api_key = configuration.get('api_key')
    if not api_key:
        log.severe("API key is missing from configuration")
        return []

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
                "equivalent_walking_distance": "FLOAT",
                "average_met": "FLOAT",
                "high_activity_met_minutes": "FLOAT",
                "medium_activity_met_minutes": "FLOAT",
                "low_activity_met_minutes": "FLOAT",
                "non_wear_time": "INT",
                "inactivity_alerts": "INT",
                "activity_score": "FLOAT",
                "rest_time": "INT",
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
                "time_in_bed": "INT",
                "awake_time": "INT",
                "light_sleep_duration": "INT",
                "rem_sleep_duration": "INT",
                "deep_sleep_duration": "INT",
                "sleep_score": "FLOAT",
                "sleep_efficiency": "FLOAT",
                "latency": "INT",
                "bedtime_start": "STRING",
                "bedtime_end": "STRING",
                "average_resting_heart_rate": "FLOAT",
                "lowest_resting_heart_rate": "INT",
                "average_hrv": "FLOAT",
                "temperature_deviation": "FLOAT",
                "last_modified": "STRING"
            }
        }
    ]

def safe_score(contributors, key):
    """Safely extract a score value and ensure it's a number"""
    try:
        value = contributors.get(key, 0)
        return float(value) if isinstance(value, (int, float)) else 0.0
    except Exception:
        return 0.0

def get_api_key(configuration):
    """Retrieve the API key from the configuration."""
    api_key = configuration.get('api_key')
    if not api_key:
        raise KeyError("Missing api_key in configuration")
    return str(api_key)

def update(configuration: dict, state: dict):
    """Extract data from the source and yield operations"""
    try:
        # Get API key from configuration
        api_key = get_api_key(configuration)

        # Setup API client headers
        headers = {"Authorization": f"Bearer {api_key}"}

        # Get start date from state or use default (30 days ago)
        default_start_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        start_date = state.get('last_sync_date', default_start_date)

        # Set current time for checkpoint updates
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Sync daily activity data
        try:
            log.info(f"Syncing daily activity data since {start_date}")

            # API endpoint for daily activity
            url = "https://api.ouraring.com/v2/usercollection/daily_activity"

            # Request parameters
            params = {
                "start_date": start_date,
                "end_date": current_date
            }

            # Make API request
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()

            # Process activity data
            for activity in data.get('data', []):
                activity_id = activity.get('id')

                # Extract and normalize data
                record = {
                    "id": activity_id,
                    "date": activity.get('day'),
                    "steps": int(activity.get('steps', 0)),
                    "total_calories": int(activity.get('calories', 0)),
                    "active_calories": int(activity.get('active_calories', 0)),
                    "equivalent_walking_distance": float(activity.get('equivalent_walking_distance', 0.0)),
                    "average_met": float(activity.get('average_met', 0.0)),
                    "high_activity_met_minutes": float(activity.get('high_activity_met_minutes', 0.0)),
                    "medium_activity_met_minutes": float(activity.get('medium_activity_met_minutes', 0.0)),
                    "low_activity_met_minutes": float(activity.get('low_activity_met_minutes', 0.0)),
                    "non_wear_time": int(activity.get('non_wear_time', 0)),
                    "inactivity_alerts": int(activity.get('inactivity_alerts', 0)),
                    "activity_score": float(activity.get('score', 0.0)),
                    "rest_time": int(activity.get('rest_time', 0)),
                    "last_modified": activity.get('timestamp')
                }

                # Yield update operation
                yield op.update("daily_activity", record)

            # Checkpoint after processing all activity data
            yield op.checkpoint({"last_sync_date": current_date})

        except Exception as e:
            log.severe(f"Error syncing daily activity: {str(e)}")

        # Sync daily sleep data
        try:
            log.info(f"Syncing daily sleep data since {start_date}")

            # API endpoint for daily sleep
            url = "https://api.ouraring.com/v2/usercollection/daily_sleep"

            # Request parameters
            params = {
                "start_date": start_date,
                "end_date": current_date
            }

            # Make API request
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()

            # Process sleep data
            for sleep in data.get('data', []):
                sleep_id = sleep.get('id')

                # Extract and normalize data
                contributors = sleep.get('contributors', {})

                record = {
                    "id": sleep_id,
                    "date": sleep.get('day'),
                    "total_sleep_duration": int(sleep.get('total_sleep_duration', 0)),
                    "time_in_bed": int(sleep.get('time_in_bed', 0)),
                    "awake_time": int(sleep.get('awake_time', 0)),
                    "light_sleep_duration": int(sleep.get('light_sleep_duration', 0)),
                    "rem_sleep_duration": int(sleep.get('rem_sleep_duration', 0)),
                    "deep_sleep_duration": int(sleep.get('deep_sleep_duration', 0)),
                    "sleep_score": float(sleep.get('score', 0.0)),
                    "sleep_efficiency": float(sleep.get('efficiency', 0.0)),
                    "latency": int(sleep.get('latency', 0)),
                    "bedtime_start": sleep.get('bedtime_start'),
                    "bedtime_end": sleep.get('bedtime_end'),
                    "average_resting_heart_rate": float(sleep.get('average_resting_heart_rate', 0.0)),
                    "lowest_resting_heart_rate": int(sleep.get('lowest_resting_heart_rate', 0)),
                    "average_hrv": float(sleep.get('average_hrv', 0.0)),
                    "temperature_deviation": float(sleep.get('temperature_deviation', 0.0)),
                    "last_modified": sleep.get('timestamp')
                }

                # Yield update operation
                yield op.update("daily_sleep", record)

            # Final checkpoint
            yield op.checkpoint({"last_sync_date": current_date})

        except Exception as e:
            log.severe(f"Error syncing daily sleep: {str(e)}")

    except Exception as e:
        log.severe(f"Error during update: {str(e)}")

# Create connector instance
connector = Connector(update=update, schema=schema)

# Entry point for debugging
if __name__ == "__main__":
    connector.debug()