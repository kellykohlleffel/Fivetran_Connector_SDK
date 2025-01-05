from datetime import datetime  # Import datetime for handling date and time conversions.
import requests as rq  # Import the requests module for making HTTP requests, aliased as rq.
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # Import the Connector class from the fivetran_connector_sdk module.
from fivetran_connector_sdk import Logging as log  # Import the Logging class from the fivetran_connector_sdk module, aliased as log.
from fivetran_connector_sdk import Operations as op  # Import the Operations class from the fivetran_connector_sdk module, aliased as op.

# Define the schema function which lets you configure the schema your connector delivers.
def schema(configuration: dict):
    return [
        {
            "table": "period",  # Name of the table in the destination.
            "primary_key": ["startTime"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "name": "STRING",  # String column for the period name.
                "startTime": "UTC_DATETIME",  # UTC date-time column for the start time.
                "endTime": "UTC_DATETIME",  # UTC date-time column for the end time.
                "temperature": "INT",  # Integer column for the temperature.
            },
        }
    ]

# Define a helper function to convert a string to a datetime object.
def str2dt(incoming: str) -> datetime:
    return datetime.strptime(incoming, "%Y-%m-%dT%H:%M:%S%z")

# Define the update function, which is a required function, and is called by Fivetran during each sync.
def update(configuration: dict, state: dict):
    cursor = state['startTime'] if 'startTime' in state else '0001-01-01T00:00:00Z'

    # Get weather forecast for Cypress, TX from National Weather Service API.
    response = rq.get("https://api.weather.gov/gridpoints/HGX/52,106/forecast")
    data = response.json()
    periods = data['properties']['periods']
    log.info(f"number of periods={len(periods)}")

    # Print table header
    print("\n--- Processing and Printing Synced Data ---")
    print(f"{'Name':<15} {'Start Time':<25} {'End Time':<25} {'Temperature':<10}")
    print("-" * 80)

    for period in periods:
        if str2dt(period['startTime']) < str2dt(cursor):
            continue

        # Extract period details
        name = period["name"]
        start_time = period["startTime"]
        end_time = period["endTime"]
        temperature = period["temperature"]

        # Print each row as it is processed
        print(f"{name:<15} {start_time:<25} {end_time:<25} {temperature:<10}")

        log.fine(f"period={period['name']}")

        yield op.upsert(table="period",
                        data={
                            "name": period["name"],
                            "startTime": period["startTime"],
                            "endTime": period["endTime"],
                            "temperature": period["temperature"]
                        })

        cursor = period['endTime']

    yield op.checkpoint(state={"startTime": cursor})

# Create the connector object.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
if __name__ == "__main__":
    print("Running the connector...")
    connector.debug()
    print("Connector run complete.")
