import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from datetime import datetime
import json
from typing import Dict, List, Tuple


def schema(configuration: dict):
    """
    Define the table schemas that Fivetran will use.
    """
    return [
        {"table": "vehicle_recalls", "primary_key": ["recall_id"]},
    ]


def rate_limit_api_call(url: str, params: Dict = None) -> Dict:
    """
    Make an API call with rate limiting.
    """
    max_retries = 3
    retry_delay = 1  # seconds

    for attempt in range(max_retries):
        try:
            log.info(f"Making API request to: {url}")
            response = rq.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except rq.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                log.info(f"API request failed: {str(e)}")
                return {}
            time.sleep(retry_delay * (attempt + 1))
    return {}


def get_vehicle_recalls(make: str, model: str, year: int) -> List[Dict]:
    """
    Get recall information for a specific make/model/year.
    """
    url = "https://api.nhtsa.gov/recalls/recallsByVehicle"
    params = {"make": make, "model": model, "modelYear": year, "format": "json"}

    response_data = rate_limit_api_call(url, params)
    results = response_data.get("results", [])
    if not results:
        log.info(f"No recalls found for {make} {model} {year}")
    return results

def update(configuration: dict, state: dict):
    try:
        # Extract parameters from configuration with explicit defaults
        make_name = "toyota"  # Input the make you want to use for recall data
        model_filter = "tundra"  # Input the model you want to use for recall data
        start_year = 2020  # Input the start year model you want to use for recall data
        end_year = 2025  # Input the end year model you want to use for recall data

        log.info(f"Using connector.py vehicle configuration: make={make_name}, model={model_filter}, start_year={start_year}, end_year={end_year}")

        # Process recalls for the specified range
        for year in range(start_year, end_year + 1):
            log.info(f"Processing recalls for {make_name} {model_filter} in year {year}...")
            recalls = get_vehicle_recalls(make_name, model_filter, year)

            for recall in recalls:
                recall_id = recall.get("NHTSACampaignNumber")
                if not recall_id:
                    continue

                # Prepare recall record with make and model
                recall_record = {
                    "recall_id": recall_id,
                    "make_name": recall.get("MakeName", make_name),
                    "model_name": recall.get("ModelName", model_filter),
                    "campaign_number": recall.get("NHTSACampaignNumber"),
                    "report_received_date": recall.get("ReportReceivedDate"),
                    "component": recall.get("Component"),
                    "summary": recall.get("Summary"),
                    "consequence": recall.get("Consequence"),
                    "remedy": recall.get("Remedy"),
                    "notes": recall.get("Notes"),
                }
                yield op.upsert("vehicle_recalls", recall_record)
                log.info(f"Upserted recall: {recall_record}")

        # Update state
        yield op.checkpoint(state={"last_sync": datetime.now().isoformat()})

    except Exception as e:
        log.info(f"Error during update: {e}")
        raise

# Create connector instance
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    print("Running the NHTSA Vehicle Data connector...")
    connector.debug()
    print("Connector run complete.")