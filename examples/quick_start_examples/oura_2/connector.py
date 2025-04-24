from fivetran_connector_sdk import Connector, Operations as op, Logging as log
from typing import Dict, List, Any
import requests
from datetime import datetime, timedelta

def schema(configuration: dict) -> List[Dict]:
    """Define the table schema for Fivetran"""
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
        },
        {
            "table": "daily_stress",
            "primary_key": ["id", "day"],
            "columns": {
                "id": "STRING",
                "day": "STRING",
                "stress_high": "INT",
                "recovery_high": "INT",
                "day_summary": "STRING"
            }
        }
    ]

def update(configuration: dict, state: dict) -> List[Dict]:
    """Sync data incrementally from Oura API"""
    api_key = configuration["api_key"]
    base_url = "https://api.ouraring.com/v2/usercollection"
    headers = {"Authorization": f"Bearer {api_key}"}

    # Initialize state if not exists
    if not state:
        state = {
            "last_sync": (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "request_count": 0
        }

    start_date = state.get("last_sync")
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    tables = ["daily_sleep", "daily_activity", "daily_stress"]

    for table in tables:
        try:
            url = f"{base_url}/{table}"
            params = {
                "start_date": start_date,
                "end_date": current_time
            }

            response = requests.get(url, headers=headers, params=params)
            state["request_count"] = state.get("request_count", 0) + 1

            if response.status_code == 429:
                log.error(f"Rate limit exceeded for {table}")
                yield op.checkpoint(state)
                continue

            if response.status_code != 200:
                log.error(f"Error fetching {table}: {response.status_code}")
                continue

            data = response.json()

            for record in data.get("data", []):
                if table == "daily_sleep":
                    # Transform sleep data
                    record_data = {
                        "id": record["id"],
                        "day": record["day"],
                        "score": record.get("score"),
                        "timestamp": record["timestamp"],
                        "contributors_deep_sleep": record.get("contributors", {}).get("deep_sleep"),
                        "contributors_efficiency": record.get("contributors", {}).get("efficiency"),
                        "contributors_latency": record.get("contributors", {}).get("latency"),
                        "contributors_rem_sleep": record.get("contributors", {}).get("rem_sleep"),
                        "contributors_restfulness": record.get("contributors", {}).get("restfulness"),
                        "contributors_timing": record.get("contributors", {}).get("timing"),
                        "contributors_total_sleep": record.get("contributors", {}).get("total_sleep")
                    }
                elif table == "daily_activity":
                    # Transform activity data
                    record_data = {
                        "id": record["id"],
                        "day": record["day"],
                        "score": record.get("score"),
                        "active_calories": record["active_calories"],
                        "average_met_minutes": record["average_met_minutes"],
                        "equivalent_walking_distance": record["equivalent_walking_distance"],
                        "high_activity_met_minutes": record["high_activity_met_minutes"],
                        "high_activity_time": record["high_activity_time"],
                        "inactivity_alerts": record["inactivity_alerts"],
                        "low_activity_met_minutes": record["low_activity_met_minutes"],
                        "low_activity_time": record["low_activity_time"],
                        "medium_activity_met_minutes": record["medium_activity_met_minutes"],
                        "medium_activity_time": record["medium_activity_time"],
                        "meters_to_target": record["meters_to_target"],
                        "non_wear_time": record["non_wear_time"],
                        "resting_time": record["resting_time"],
                        "sedentary_met_minutes": record["sedentary_met_minutes"],
                        "sedentary_time": record["sedentary_time"],
                        "steps": record["steps"],
                        "target_calories": record["target_calories"],
                        "target_meters": record["target_meters"],
                        "total_calories": record["total_calories"],
                        "timestamp": record["timestamp"]
                    }
                else:  # daily_stress
                    # Transform stress data
                    record_data = {
                        "id": record["id"],
                        "day": record["day"],
                        "stress_high": record.get("stress_high"),
                        "recovery_high": record.get("recovery_high"),
                        "day_summary": record.get("day_summary")
                    }

                yield op.upsert(table, record_data)

            # Checkpoint after each table
            if state["request_count"] >= 100:
                log.info(f"Checkpointing after {state['request_count']} requests")
                yield op.checkpoint(state)
                state["request_count"] = 0

        except Exception as e:
            log.error(f"Error processing {table}: {str(e)}")
            continue

    # Update state with latest sync time
    state["last_sync"] = current_time
    yield op.checkpoint(state)

# Create connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()