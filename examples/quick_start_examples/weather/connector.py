from datetime import datetime
import requests as rq
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Define the schema function to configure the schema delivered by the connector.
def schema(configuration: dict):
    return [
        {
            "table": "period",
            "primary_key": ["startTime"],
            "columns": {
                "name": "STRING",
                "startTime": "UTC_DATETIME",
                "endTime": "UTC_DATETIME",
                "temperature": "INT",
            },
        }
    ]

# Helper function to convert string to datetime.
def str2dt(incoming: str) -> datetime:
    return datetime.strptime(incoming, "%Y-%m-%dT%H:%M:%S%z")

# The update function runs during each sync.
def update(configuration: dict, state: dict):
    try:
        cursor = state.get('startTime', '0001-01-01T00:00:00Z')
        log.info(f"Using cursor: {cursor}")

        # Fetch weather forecast data from the API.
        response = rq.get("https://api.weather.gov/gridpoints/ILM/58,40/forecast")
        response.raise_for_status()  # Ensure the request was successful.

        periods = response.json()['properties']['periods']
        log.info(f"Fetched {len(periods)} periods")

        # Process each period and upsert data if not already synced.
        for period in periods:
            if str2dt(period['startTime']) < str2dt(cursor):
                log.fine(f"Skipping period: {period['name']}")
                continue

            log.info(f"Processing period: {period['name']}")

            yield op.upsert(
                table="period",
                data={
                    "name": period["name"],
                    "startTime": period["startTime"],
                    "endTime": period["endTime"],
                    "temperature": period["temperature"],
                }
            )
            cursor = period['endTime']  # Update cursor to the latest endTime.

        # Save the state for the next sync.
        yield op.checkpoint(state={"startTime": cursor})
        log.info("Checkpoint saved successfully.")

    except Exception as e:
        log.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

# Create a connector instance with the update and schema functions.
connector = Connector(update=update, schema=schema)

# Main entry point to run the connector in debug mode.
if __name__ == "__main__":
    print("Running the connector...")
    connector.debug()
    print("Connector run complete.")

import sqlite3

# Existing code from connector.py stays the same above...

if __name__ == "__main__":
    print("Running the connector...")
    connector.debug()
    print("Connector run complete.")

    # Connect to the local SQLite database used by the Fivetran SDK tester.
    db_path = "./files/warehouse.db"
    conn = sqlite3.connect(db_path)

    # Query the 'period' table and print the results.
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tester.period")

        rows = cursor.fetchall()
        print("\n--- Contents of the 'period' Table ---")
        print(f"{'Start Time':<25} {'Name':<15} {'End Time':<25} {'Temperature':<10}")
        print("-" * 80)

        for row in rows:
            start_time, name, end_time, temperature = row
            print(f"{start_time:<25} {name:<15} {end_time:<25} {temperature:<10}")

    except sqlite3.Error as e:
        print(f"Error querying the SQLite database: {e}")
    finally:
        conn.close()
