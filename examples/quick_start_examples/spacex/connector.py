"""
connector.py

This script connects to the SpaceX API using the Fivetran Connector SDK. 
It retrieves information about past SpaceX launches, including mission name, 
launch date, rocket type, and launch site, and stores this data in Fivetran 
using the SDK's upsert operation.

Example usage: This script can be used to demonstrate pulling launch data 
from SpaceX, making it useful for showcasing how the Fivetran Connector SDK works.

Requirements:
- No additional Python libraries are required, as `requests` and the 
  `fivetran_connector_sdk` are assumed to be pre-installed.

Fivetran Connector SDK Documentation:
- Technical Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
- Best Practices: https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

from datetime import datetime  # Import datetime for handling date conversions.
import requests as rq  # Import requests for making HTTP requests, aliased as rq.

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # Connector class to set up the Fivetran connector.
from fivetran_connector_sdk import Logging as log  # Logging functionality to log key steps.
from fivetran_connector_sdk import Operations as op  # Operations class for Fivetran data operations.

# Define the schema function to configure the schema your connector delivers.
def schema(configuration: dict):
    """
    Define the table schema that Fivetran will use.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
    
    Returns:
        list: A list with schema definitions for each table to sync.
    
    Schema:
    - table: "launch"
    - primary_key: "flight_number"
    - columns:
        - flight_number (INT): Unique identifier for each launch.
        - mission_name (STRING): Name of the mission.
        - launch_date (UTC_DATETIME): Date and time of the launch.
        - rocket_name (STRING): Name of the rocket used.
        - launch_site (STRING): Name of the launch site.
    """
    return [
        {
            "table": "launch",  # Table name in the destination.
            "primary_key": ["flight_number"],  # Primary key column for deduplication.
            "columns": {  # Columns and their data types.
                "flight_number": "INT",  # Unique flight number for each launch.
                "mission_name": "STRING",  # Name of the mission.
                "launch_date": "UTC_DATETIME",  # Launch date and time.
                "rocket_name": "STRING",  # Name of the rocket.
                "launch_site": "STRING",  # Name of the launch site.
            },
        }
    ]

# Define the update function, which is called by Fivetran during each sync.
def update(configuration: dict, state: dict):
    """
    Retrieve data from the SpaceX API and send it to Fivetran.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
        state (dict): A dictionary containing the last sync state, such as cursor values.
    
    Yields:
        op.upsert: An upsert operation for each launch record.
        op.checkpoint: A checkpoint operation to save the updated state.
    
    Logic:
    - Fetch launch data from the SpaceX API.
    - Process each launch entry, extracting flight number, mission name, launch date, rocket name, and launch site.
    - Skip launches that occurred before the last synced launch date.
    - Save the latest launch date encountered to the state after each sync.
    """
    cursor = state.get("launch_date", "2000-01-01T00:00:00Z")  # Initialize cursor to a very old date.

    # Fetch data from SpaceX API for past launches.
    response = rq.get("https://api.spacexdata.com/v4/launches/past")
    data = response.json()  # Parse the JSON response.
    log.info(f"Number of launches retrieved: {len(data)}")  # Log the number of launches retrieved.

    # Print table header for visual output in debug mode.
    print("\n--- Processing and Printing Synced Data ---")
    print(f"{'Flight Number':<15} {'Mission Name':<25} {'Launch Date':<25} {'Rocket Name':<20} {'Launch Site':<20}")
    print("-" * 100)

    # Loop through each launch in the response data.
    for launch in data:
        # Extract relevant details for each launch, handling missing fields.
        flight_number = launch.get("flight_number")  # Unique flight number for the launch.
        mission_name = launch.get("name", "Unknown Mission")  # Mission name.
        launch_date = launch.get("date_utc")  # UTC launch date and time.
        rocket_name = launch.get("rocket")  # Rocket ID; further details may need another API call if desired.
        launch_site = launch.get("launchpad")  # Launch site ID; further details may need another API call.

        # Skip entries if the launch date is before the cursor.
        if launch_date < cursor:
            continue  # Skip this launch if it doesn't meet the criteria.

        # Print each processed row in the debug output.
        print(f"{flight_number:<15} {mission_name:<25} {launch_date:<25} {rocket_name:<20} {launch_site:<20}")

        # Log fine-grained details for debugging.
        log.fine(f"Flight number={flight_number}, mission_name={mission_name}")

        # Yield each launch as an upsert operation for Fivetran.
        yield op.upsert(
            table="launch",  # Table to which data is upserted.
            data={
                "flight_number": flight_number,
                "mission_name": mission_name,
                "launch_date": launch_date,
                "rocket_name": rocket_name,
                "launch_site": launch_site,
            }
        )

        # Update the cursor to the latest launch date encountered.
        cursor = max(cursor, launch_date)  # Ensure cursor holds the latest date.

    # Save the updated state with the latest launch date.
    yield op.checkpoint(state={"launch_date": cursor})  # Save the cursor to maintain sync state.

# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode when executing the script directly.
if __name__ == "__main__":
    print("Running the SpaceX connector...")
    connector.debug()  # Run the connector in debug mode to simulate a Fivetran sync.
    print("Connector run complete.")
