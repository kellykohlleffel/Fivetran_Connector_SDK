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


def load_configuration() -> dict:
    """
    Load default configuration values from spec.json.
    """
    try:
        with open("spec.json", "r") as spec_file:
            spec = json.load(spec_file)
            config = {key: prop.get("default") for key, prop in spec["schema"]["properties"].items()}
            log.info(f"Loaded configuration from spec.json: {config}")
            return config
    except Exception as e:
        log.info(f"Error loading spec.json: {str(e)}")
        return {}


def update(configuration: dict, state: dict):
    try:
        # Load configuration from spec.json if not provided
        if not configuration:
            log.info("Configuration is empty. Loading from spec.json.")
            configuration = load_configuration()

        # Extract parameters from configuration
        make_name = configuration.get("make_name", "ford").lower()
        model_filter = configuration.get("model_filter", "f-150").lower()
        start_year = configuration.get("start_year", 2022)
        end_year = configuration.get("end_year", 2022)

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
