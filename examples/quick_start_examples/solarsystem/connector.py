import requests as rq  # Import requests for making HTTP requests, aliased as rq.
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
    - table: "solar_system_object"
    - primary_key: "id"
    - columns:
        - id (STRING): Unique identifier for each object.
        - name (STRING): Name of the object (e.g., Earth, Mars).
        - type (STRING): Type of the object (e.g., planet, moon).
        - orbital_period (FLOAT): Orbital period of the object around the Sun (if applicable).
        - distance_from_sun (FLOAT): Average distance of the object from the Sun in km.
    """
    return [
        {
            "table": "solar_system_object",  # Table name in the destination.
            "primary_key": ["id"],  # Primary key column for deduplication.
            "columns": {  # Columns and their data types.
                "id": "STRING",  # Unique identifier for each object.
                "name": "STRING",  # Name of the celestial object.
                "type": "STRING",  # Type of object (e.g., planet, moon).
                "orbital_period": "FLOAT",  # Orbital period in days.
                "distance_from_sun": "FLOAT",  # Average distance from the Sun in km.
            },
        }
    ]

# Define the update function, which is called by Fivetran during each sync.
def update(configuration: dict, state: dict):
    """
    Retrieve data from the Solar System OpenData API and send it to Fivetran.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
        state (dict): A dictionary containing the last sync state.
    
    Yields:
        op.upsert: An upsert operation for each Solar System object record.
        op.checkpoint: A checkpoint operation to save the updated state.
    
    Logic:
    - Fetch data on Solar System objects from the Solar System OpenData API.
    - Process each entry, extracting details such as object ID, name, type, orbital period, and distance from the Sun.
    """
    # Fetch data from Solar System OpenData API for celestial objects.
    response = rq.get("https://api.le-systeme-solaire.net/rest/bodies/")
    data = response.json()  # Parse the JSON response.
    objects = data.get("bodies", [])  # Access the list of Solar System objects.
    log.info(f"Number of objects retrieved: {len(objects)}")  # Log the number of objects retrieved.

    # Print table header for visual output in debug mode.
    print("\n--- Processing and Printing Synced Data ---")
    print(f"{'ID':<10} {'Name':<25} {'Type':<15} {'Orbital Period (days)':<20} {'Distance from Sun (km)':<25}")
    print("-" * 95)

    # Loop through each object in the response data.
    for obj in objects:
        # Extract relevant details for each object, handling missing fields.
        object_id = obj.get("id", "Unknown ID")  # Unique object ID.
        name = obj.get("englishName", "Unknown Name")  # Object name.
        type_ = obj.get("bodyType", "Unknown Type")  # Type of object (e.g., planet, moon).
        orbital_period = obj.get("sideralOrbit", None)  # Orbital period around the Sun.
        distance_from_sun = obj.get("semimajorAxis", None)  # Distance from Sun in km.

        # Print each processed row in the debug output.
        print(f"{object_id:<10} {name:<25} {type_:<15} {orbital_period:<20} {distance_from_sun:<25}")

        # Log fine-grained details for debugging.
        log.fine(f"Object ID={object_id}, name={name}")

        # Yield each object as an upsert operation for Fivetran.
        yield op.upsert(
            table="solar_system_object",  # Table to which data is upserted.
            data={
                "id": object_id,
                "name": name,
                "type": type_,
                "orbital_period": orbital_period,
                "distance_from_sun": distance_from_sun,
            }
        )

    # Save the checkpoint state if needed (this API does not use a cursor-based sync).
    yield op.checkpoint(state={})  # Keep the state empty since this API doesn’t need a cursor.

# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode when executing the script directly.
if __name__ == "__main__":
    print("Running the Solar System OpenData connector...")
    connector.debug()  # Run the connector in debug mode to simulate a Fivetran sync.
    print("Connector run complete.")
