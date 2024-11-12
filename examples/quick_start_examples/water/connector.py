"""
connector.py

This script connects to the USGS Water Data for the Nation API using the Fivetran Connector SDK.
It retrieves real-time water measurements for Texas, focusing on conditions relevant to kayak fishing trips.
The data is stored in Fivetran using the SDK's upsert operation.

Example usage: This script demonstrates pulling water data from the USGS API, including parameters like
discharge and temperature, for tracking water conditions in Texas.

Requirements:
- No additional Python libraries are required, as `requests` and the 
  `fivetran_connector_sdk` are assumed to be pre-installed.
"""

import requests as rq  # Import requests for making HTTP requests, aliased as rq.
from fivetran_connector_sdk import Connector  # Connector class to set up the Fivetran connector.
from fivetran_connector_sdk import Logging as log  # Logging functionality to log key steps.
from fivetran_connector_sdk import Operations as op  # Operations class for Fivetran data operations.

# Define the schema function for the Measurements table.
def schema(configuration: dict):
    """
    Define the table schema that Fivetran will use.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
    
    Returns:
        list: A list with schema definitions for the Measurements table.
    """
    return [
        {
            "table": "measurements",
            "primary_key": ["measurement_id"],
            "columns": {
                "measurement_id": "STRING",
                "site_code": "STRING",
                "date_time": "UTC_DATETIME",
                "parameter": "STRING",
                "value": "FLOAT",
                "unit": "STRING",
            },
        },
    ]

# Define the update function, focusing on retrieving Measurements data.
def update(configuration: dict, state: dict):
    """
    Retrieve data from the USGS API and send it to Fivetran.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
        state (dict): A dictionary containing the last sync state.
    
    Yields:
        op.upsert: An upsert operation for each record.
        op.checkpoint: A checkpoint operation to save the updated state.
    """
    
    # Fetch and yield measurement data
    endpoint_measurements = "https://waterservices.usgs.gov/nwis/iv/"
    params_measurements = {
        "stateCd": "TX",
        "format": "json",
        "parameterCd": "00060,00010",  # Discharge and temperature
        "siteStatus": "active",
        "period": "PT6H"  # Last 6 hours for testing
    }
    
    response_measurements = rq.get(endpoint_measurements, params=params_measurements)
    
    if response_measurements.status_code == 200:
        data_measurements = response_measurements.json().get("value", {}).get("timeSeries", [])
        log.info(f"Number of measurements retrieved: {len(data_measurements)}")

        for i, measurement in enumerate(data_measurements):
            log.info(f"Processing measurement {i + 1}/{len(data_measurements)}")  # Progress log

            site_code = (
                measurement.get("sourceInfo", {}).get("siteCode", [{}])[0].get("value")
                if measurement.get("sourceInfo", {}).get("siteCode")
                else "Unknown Site Code"
            )
            measurement_id = f"{site_code}_{measurement.get('variable', {}).get('variableCode', [{}])[0].get('value', '')}"
            date_time = (
                measurement.get("values", [{}])[0].get("value", [{}])[0].get("dateTime")
                if measurement.get("values") and measurement.get("values")[0].get("value")
                else None
            )
            parameter = measurement.get("variable", {}).get("variableName", "Unknown Parameter")
            value = (
                measurement.get("values", [{}])[0].get("value", [{}])[0].get("value")
                if measurement.get("values") and measurement.get("values")[0].get("value")
                else None
            )
            unit = measurement.get("variable", {}).get("unit", {}).get("unitCode", "Unknown Unit")

            yield op.upsert(
                table="measurements",
                data={
                    "measurement_id": measurement_id,
                    "site_code": site_code,
                    "date_time": date_time,
                    "parameter": parameter,
                    "value": float(value) if value else None,
                    "unit": unit,
                }
            )

    else:
        log.info(f"Measurements API request failed with status code {response_measurements.status_code}")

    # Save checkpoint state if needed (this API does not use a cursor-based sync).
    yield op.checkpoint(state={})

# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode
if __name__ == "__main__":
    print("Running the USGS Water Data connector (Measurements table)...")
    connector.debug()  # Run the connector in debug mode to simulate a Fivetran sync.
    print("Connector run complete.")
