"""
connector.py

This script connects to the USGS Water Data for the Nation API using the Fivetran Connector SDK.
It retrieves real-time water data for Texas, including information on lakes, rivers, and streams.
The data is stored in Fivetran using the SDK's upsert operation.

Example usage: This script demonstrates pulling water data from the USGS API, which is valuable
for tracking water conditions relevant to kayak fishing trips.

Configuration:
- Set the `LIMIT` variable to control the number of records retrieved per table.

Requirements:
- No additional Python libraries are required, as `requests` and the 
  `fivetran_connector_sdk` are assumed to be pre-installed.
"""

import requests as rq  # Import requests for making HTTP requests, aliased as rq.
from fivetran_connector_sdk import Connector  # Connector class to set up the Fivetran connector.
from fivetran_connector_sdk import Logging as log  # Logging functionality to log key steps.
from fivetran_connector_sdk import Operations as op  # Operations class for Fivetran data operations.

# Set the record retrieval limit for all API requests.
LIMIT = 10  # Maximum number of records retrieved per table

# Define the schema function to configure the schema your connector delivers.
def schema(configuration: dict):
    """
    Define the table schemas that Fivetran will use.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
    
    Returns:
        list: A list with schema definitions for each table to sync.
    """
    return [
        {
            "table": "stations",
            "primary_key": ["site_code"],
            "columns": {
                "site_code": "STRING",
                "name": "STRING",
                "latitude": "FLOAT",
                "longitude": "FLOAT",
                "county": "STRING",
                "water_body_type": "STRING",
            },
        },
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

# Define the update function, which is called by Fivetran during each sync.
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
    
    # Fetch and yield station data
    endpoint_stations = "https://waterservices.usgs.gov/nwis/site"
    params_stations = {
        "stateCd": "TX",
        "format": "json",
        "siteType": "ST",
        "hasDataTypeCd": "iv",
        "parameterCd": "00060,00010",
        "siteStatus": "active"
    }
    
    response_stations = rq.get(endpoint_stations, params=params_stations)
    
    if response_stations.status_code == 200:
        data_stations = response_stations.json().get("value", {}).get("site", [])
        log.info(f"Number of stations retrieved: {len(data_stations)}")

        for station in data_stations:
            site_code = station.get("siteCode", [{}])[0].get("value", "Unknown Site Code")
            name = station.get("siteName", "Unknown Name")
            latitude = station.get("geoLocation", {}).get("geogLocation", {}).get("latitude", None)
            longitude = station.get("geoLocation", {}).get("geogLocation", {}).get("longitude", None)
            county = station.get("countyCd", "Unknown County")
            water_body_type = station.get("siteTypeCd", "Unknown Type")

            yield op.upsert(
                table="stations",
                data={
                    "site_code": site_code,
                    "name": name,
                    "latitude": float(latitude) if latitude else None,
                    "longitude": float(longitude) if longitude else None,
                    "county": county,
                    "water_body_type": water_body_type,
                }
            )
    else:
        log.error(f"Stations API request failed with status code {response_stations.status_code}")

    # Fetch and yield measurement data for each station
    endpoint_measurements = "https://waterservices.usgs.gov/nwis/iv/"
    params_measurements = {
        "stateCd": "TX",
        "format": "json",
        "parameterCd": "00060,00010",  # Discharge and temperature
        "siteStatus": "active",
        "period": "P1D"  # Last 24 hours
    }
    
    response_measurements = rq.get(endpoint_measurements, params=params_measurements)
    
    if response_measurements.status_code == 200:
        data_measurements = response_measurements.json().get("value", {}).get("timeSeries", [])
        log.info(f"Number of measurements retrieved: {len(data_measurements)}")

        for measurement in data_measurements:
            site_code = measurement.get("sourceInfo", {}).get("siteCode", [{}])[0].get("value", "Unknown Site Code")
            measurement_id = f"{site_code}_{measurement.get('variable', {}).get('variableCode', [{}])[0].get('value', '')}"
            date_time = measurement.get("values", [{}])[0].get("value", [{}])[0].get("dateTime", None)
            parameter = measurement.get("variable", {}).get("variableName", "Unknown Parameter")
            value = measurement.get("values", [{}])[0].get("value", [{}])[0].get("value", None)
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
        log.error(f"Measurements API request failed with status code {response_measurements.status_code}")

    # Save checkpoint state if needed (this API does not use a cursor-based sync).
    yield op.checkpoint(state={})

# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode
if __name__ == "__main__":
    print("Running the USGS Water Data connector (Stations and Measurements tables)...")
    connector.debug()  # Run the connector in debug mode to simulate a Fivetran sync.
    print("Connector run complete.")
