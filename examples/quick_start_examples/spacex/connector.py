"""
connector.py

This script connects to the SpaceX API using the Fivetran Connector SDK.
It retrieves information about SpaceX launches, rockets, and capsules,
storing this data in Fivetran using the SDK's upsert operation.

Example usage: This script can be used to sync SpaceX data into your data warehouse,
demonstrating how the Fivetran Connector SDK works with REST APIs.

Requirements:
- No additional Python libraries are required, as `requests` and the 
  `fivetran_connector_sdk` are assumed to be pre-installed.

SpaceX API Documentation:
- API Reference: https://github.com/r-spacex/SpaceX-API/tree/master/docs
"""

import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

def schema(configuration: dict):
    """
    Define the table schemas that Fivetran will use.

    Args:
        configuration (dict): Configuration settings for the connector.
    
    Returns:
        list: Schema definitions for each table to sync.
    """
    return [
        {
            "table": "launches",
            "primary_key": ["id"]
        },
        {
            "table": "rockets",
            "primary_key": ["id"]
        },
        {
            "table": "capsules",
            "primary_key": ["id"]
        }
    ]

def update(configuration: dict, state: dict):
    """
    Retrieve data from the SpaceX API and send it to Fivetran.

    Args:
        configuration (dict): Configuration settings for the connector.
        state (dict): Last sync state containing timestamps.
    
    Yields:
        op.upsert: Upsert operations for each record.
        op.checkpoint: Checkpoint operation to save the sync state.
    """
    base_url = "https://api.spacexdata.com/v4"

    # Process launches
    log.info("Fetching launches data...")
    launches_response = rq.get(f"{base_url}/launches")
    launches = launches_response.json()
    
    print("\n--- Processing Launches Data ---")
    print(f"{'Flight #':<8} {'Name':<30} {'Date':<25} {'Success':<8}")
    print("-" * 71)

    for launch in launches:
        print(f"{launch.get('flight_number', 'N/A'):<8} "
              f"{launch.get('name', 'Unknown'):<30} "
              f"{launch.get('date_utc', 'N/A'):<25} "
              f"{str(launch.get('success', 'N/A')):<8}")

        yield op.upsert(
            table="launches",
            data={
                "id": launch.get("id"),
                "flight_number": launch.get("flight_number"),
                "name": launch.get("name"),
                "date_utc": launch.get("date_utc"),
                "success": launch.get("success"),
                "details": launch.get("details"),
                "rocket_id": launch.get("rocket"),
                "launchpad_id": launch.get("launchpad")
            }
        )

    # Process rockets
    log.info("Fetching rockets data...")
    rockets_response = rq.get(f"{base_url}/rockets")
    rockets = rockets_response.json()

    print("\n--- Processing Rockets Data ---")
    print(f"{'Name':<20} {'Type':<15} {'Active':<8} {'Success Rate':<12}")
    print("-" * 55)

    for rocket in rockets:
        print(f"{rocket.get('name', 'Unknown'):<20} "
              f"{rocket.get('type', 'N/A'):<15} "
              f"{str(rocket.get('active', 'N/A')):<8} "
              f"{str(rocket.get('success_rate_pct', 'N/A')):<12}")

        yield op.upsert(
            table="rockets",
            data={
                "id": rocket.get("id"),
                "name": rocket.get("name"),
                "type": rocket.get("type"),
                "active": rocket.get("active"),
                "stages": rocket.get("stages"),
                "boosters": rocket.get("boosters"),
                "cost_per_launch": rocket.get("cost_per_launch"),
                "success_rate_pct": rocket.get("success_rate_pct"),
                "description": rocket.get("description")
            }
        )

    # Process capsules
    log.info("Fetching capsules data...")
    capsules_response = rq.get(f"{base_url}/dragons")
    capsules = capsules_response.json()

    print("\n--- Processing Capsules Data ---")
    print(f"{'Serial':<15} {'Type':<15} {'Status':<15} {'Reuse Count':<12}")
    print("-" * 57)

    for capsule in capsules:
        print(f"{capsule.get('serial', 'Unknown'):<15} "
              f"{capsule.get('type', 'N/A'):<15} "
              f"{capsule.get('status', 'N/A'):<15} "
              f"{str(capsule.get('reuse_count', 'N/A')):<12}")

        yield op.upsert(
            table="capsules",
            data={
                "id": capsule.get("id"),
                "serial": capsule.get("serial"),
                "status": capsule.get("status"),
                "type": capsule.get("type"),
                "last_update": capsule.get("last_update"),
                "reuse_count": capsule.get("reuse_count"),
                "water_landings": capsule.get("water_landings"),
                "land_landings": capsule.get("land_landings")
            }
        )

    # Save checkpoint state
    yield op.checkpoint(state={})

# Create connector instance
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    print("Running the SpaceX API connector...")
    connector.debug()
    print("Connector run complete.")