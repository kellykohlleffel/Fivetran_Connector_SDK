from fivetran_connector_sdk import Connector, Operations as op, Logging as log
from typing import Dict, Any
import requests
from datetime import datetime, timezone
from dateutil import parser
import time

BASE_URL = "https://api.ouraring.com/v2/usercollection"

def normalize_datetime(date_str: str) -> str:
    """Normalize datetime string to RFC 3339 format."""
    try:
        if 'T' not in date_str:
            # Handle date-only strings
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            # Handle datetime strings with potential timezone
            return parser.parse(date_str).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        log.error(f"Error normalizing datetime {date_str}: {str(e)}")
        return date_str

def make_request(endpoint: str, config: Dict[str, Any], params: Dict[str, Any] = None) -> Dict:
    """Make authenticated request to Oura API with retry logic."""
    headers = {"Authorization": f"Bearer {config['api_key']}"}
    max_retries = 3
    base_delay = 1

    for attempt in range(max_retries):
        try:
            response = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, params=params)

            if response.status_code == 429:
                delay = base_delay * (2 ** attempt)
                log.warning(f"Rate limited. Waiting {delay} seconds...")
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise Exception(f"API request failed after {max_retries} attempts: {str(e)}")
            delay = base_delay * (2 ** attempt)
            time.sleep(delay)

    raise Exception("Max retries exceeded")

def schema(config: Dict[str, Any]) -> list:
    """Define the connector schema."""
    return [
        {
            "table": "daily_sleep",
            "primary_key": ["id", "day"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "score": "INT",
                "timestamp": "UTC_DATETIME",
                "contributors_deep_sleep": "INT",
                "contributors_efficiency": "INT",
                "contributors_latency": "INT",
                "contributors_rem_sleep": "INT",
                "contributors_restfulness": "INT",
                "contributors_timing": "INT",
                "contributors_total_sleep": "INT"
            }
        },
        {
            "table": "daily_activity",
            "primary_key": ["id", "day"],
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
                "sedentary_met_minutes": "INT",
                "sedentary_time": "INT",
                "steps": "INT",
                "target_calories": "INT",
                "target_meters": "INT",
                "total_calories": "INT",
                "timestamp": "UTC_DATETIME"
            }
        }
    ]

def update(config: Dict[str, Any], state: Dict[str, Any]) -> None:
    """Incrementally update data from Oura API."""
    start_date = "2025-01-01"
    end_date = "2025-04-20"

    # Track request count for rate limiting
    request_count = state.get('request_count', 0)

    # Update daily sleep data
    try:
        sleep_data = make_request("daily_sleep", config, {
            "start_date": start_date,
            "end_date": end_date
        })

        for sleep in sleep_data.get("data", []):
            yield op.upsert(
                "daily_sleep",
                {
                    "id": sleep["id"],
                    "day": sleep["day"],
                    "score": sleep.get("score"),
                    "timestamp": normalize_datetime(sleep["timestamp"]),
                    "contributors_deep_sleep": sleep.get("contributors", {}).get("deep_sleep"),
                    "contributors_efficiency": sleep.get("contributors", {}).get("efficiency"),
                    "contributors_latency": sleep.get("contributors", {}).get("latency"),
                    "contributors_rem_sleep": sleep.get("contributors", {}).get("rem_sleep"),
                    "contributors_restfulness": sleep.get("contributors", {}).get("restfulness"),
                    "contributors_timing": sleep.get("contributors", {}).get("timing"),
                    "contributors_total_sleep": sleep.get("contributors", {}).get("total_sleep")
                }
            )

        request_count += 1
        yield op.checkpoint({"request_count": request_count})

    except Exception as e:
        log.error(f"Error fetching sleep data: {str(e)}")
        raise

    # Update daily activity data
    try:
        activity_data = make_request("daily_activity", config, {
            "start_date": start_date,
            "end_date": end_date
        })

        for activity in activity_data.get("data", []):
            yield op.upsert(
                "daily_activity",
                {
                    "id": activity["id"],
                    "day": activity["day"],
                    "score": activity.get("score"),
                    "active_calories": activity.get("active_calories"),
                    "average_met_minutes": activity.get("average_met_minutes"),
                    "equivalent_walking_distance": activity.get("equivalent_walking_distance"),
                    "high_activity_met_minutes": activity.get("high_activity_met_minutes"),
                    "high_activity_time": activity.get("high_activity_time"),
                    "inactivity_alerts": activity.get("inactivity_alerts"),
                    "low_activity_met_minutes": activity.get("low_activity_met_minutes"),
                    "low_activity_time": activity.get("low_activity_time"),
                    "medium_activity_met_minutes": activity.get("medium_activity_met_minutes"),
                    "medium_activity_time": activity.get("medium_activity_time"),
                    "meters_to_target": activity.get("meters_to_target"),
                    "non_wear_time": activity.get("non_wear_time"),
                    "resting_time": activity.get("resting_time"),
                    "sedentary_met_minutes": activity.get("sedentary_met_minutes"),
                    "sedentary_time": activity.get("sedentary_time"),
                    "steps": activity.get("steps"),
                    "target_calories": activity.get("target_calories"),
                    "target_meters": activity.get("target_meters"),
                    "total_calories": activity.get("total_calories"),
                    "timestamp": normalize_datetime(activity["timestamp"])
                }
            )

        request_count += 1
        yield op.checkpoint({"request_count": request_count})

    except Exception as e:
        log.error(f"Error fetching activity data: {str(e)}")
        raise

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()