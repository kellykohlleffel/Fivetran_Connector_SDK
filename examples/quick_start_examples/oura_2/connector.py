from fivetran_connector_sdk import Connector, Operations as op, Logging as log
from typing import Dict, List, Any
import requests
from datetime import datetime, timedelta
import time

def schema(configuration: dict) -> List[Dict]:
    """Define the table schema for Fivetran"""
    return [
        {
            "table": "personal_info",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "age": "INT",
                "weight": "FLOAT",
                "height": "FLOAT", 
                "biological_sex": "STRING",
                "email": "STRING"
            }
        },
        {
            "table": "daily_activity",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "UTC_DATETIME",
                "timestamp": "UTC_DATETIME",
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
            "table": "sleep",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "day": "UTC_DATETIME",
                "bedtime_start": "UTC_DATETIME",
                "bedtime_end": "UTC_DATETIME",
                "average_breath": "FLOAT",
                "average_heart_rate": "FLOAT",
                "average_hrv": "INT",
                "awake_time": "INT",
                "deep_sleep_duration": "INT",
                "efficiency": "INT",
                "latency": "INT",
                "light_sleep_duration": "INT",
                "low_battery_alert": "BOOLEAN",
                "lowest_heart_rate": "INT",
                "period": "INT",
                "readiness_score_delta": "INT",
                "rem_sleep_duration": "INT",
                "restless_periods": "INT",
                "sleep_score_delta": "INT",
                "time_in_bed": "INT",
                "total_sleep_duration": "INT",
                "type": "STRING"
            }
        }
    ]

def update(configuration: dict, state: dict) -> List[Dict]:
    """Sync data incrementally from Oura API"""
    base_url = "https://api.ouraring.com/v2/usercollection"
    headers = {"Authorization": f"Bearer {configuration['api_key']}"}

    # Track request count for rate limiting
    request_count = 0

    # Initialize state if empty
    if not state:
        state = {
            "last_sync": "2020-01-01T00:00:00Z",
            "request_count": 0
        }

    last_sync = state.get("last_sync")
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        # Sync personal info
        response = requests.get(f"{base_url}/personal_info", headers=headers)
        response.raise_for_status()
        request_count += 1

        if response.status_code == 200:
            data = response.json()
            yield op.upsert("personal_info", data)
            yield op.checkpoint({"last_sync": current_time, "request_count": request_count})

        # Sync daily activity
        response = requests.get(
            f"{base_url}/daily_activity",
            params={"start_date": last_sync},
            headers=headers
        )
        response.raise_for_status()
        request_count += 1

        if response.status_code == 200:
            data = response.json()
            for activity in data.get("data", []):
                yield op.upsert("daily_activity", activity)
            yield op.checkpoint({"last_sync": current_time, "request_count": request_count})

        # Sync sleep data
        response = requests.get(
            f"{base_url}/sleep",
            params={"start_date": last_sync},
            headers=headers
        )
        response.raise_for_status()
        request_count += 1

        if response.status_code == 200:
            data = response.json()
            for sleep in data.get("data", []):
                yield op.upsert("sleep", sleep)
            yield op.checkpoint({"last_sync": current_time, "request_count": request_count})

    except requests.exceptions.RequestException as e:
        log.severe(f"API request failed: {str(e)}")
        raise

    except Exception as e:
        log.severe(f"Unexpected error: {str(e)}")
        raise

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()