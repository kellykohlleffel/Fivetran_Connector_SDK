"""
connector.py

This script connects to TheMealDB API using the Fivetran Connector SDK. 
It retrieves information about meals, including meal name, category, cuisine, 
instructions, and main ingredients, and stores this data in Fivetran 
using the SDK's upsert operation.

Example usage: This script can be used to demonstrate pulling meal data 
from TheMealDB, making it useful for showcasing how the Fivetran Connector SDK works.

Requirements:
- No additional Python libraries are required, as `requests` and the 
  `fivetran_connector_sdk` are assumed to be pre-installed.

Fivetran Connector SDK Documentation:
- Technical Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
- Best Practices: https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

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
    - table: "meal"
    - primary_key: "meal_id"
    - columns:
        - meal_id (INT): Unique identifier for each meal.
        - meal_name (STRING): Name of the meal.
        - category (STRING): Category of the meal (e.g., "Dessert", "Main Course").
        - area (STRING): Cuisine or origin of the meal (e.g., "Italian", "American").
        - instructions (STRING): Cooking instructions.
        - ingredients (STRING): Main ingredients concatenated as a single string.
    """
    return [
        {
            "table": "meal",  # Table name in the destination.
            "primary_key": ["meal_id"],  # Primary key column for deduplication.
            "columns": {  # Columns and their data types.
                "meal_id": "INT",  # Unique identifier for each meal.
                "meal_name": "STRING",  # Name of the meal.
                "category": "STRING",  # Meal category.
                "area": "STRING",  # Cuisine or origin.
                "instructions": "STRING",  # Cooking instructions.
                "ingredients": "STRING",  # Ingredients as a single concatenated string.
            },
        }
    ]

# Define the update function, which is called by Fivetran during each sync.
def update(configuration: dict, state: dict):
    """
    Retrieve data from TheMealDB API and send it to Fivetran.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
        state (dict): A dictionary containing the last sync state.
    
    Yields:
        op.upsert: An upsert operation for each meal record.
        op.checkpoint: A checkpoint operation to save the updated state.
    
    Logic:
    - Fetch meal data from TheMealDB API.
    - Process each meal entry, extracting meal ID, name, category, cuisine, instructions, and ingredients.
    """
    
    # Fetch meal data from TheMealDB API
    response = rq.get("https://www.themealdb.com/api/json/v1/1/search.php?s=")  # Get all meals
    data = response.json()
    meals = data.get("meals", [])
    log.info(f"Number of meals retrieved: {len(meals)}")  # Log the number of meals retrieved.

    # Print table header for visual output in debug mode.
    print("\n--- Processing and Printing Synced Data ---")
    print(f"{'Meal ID':<10} {'Meal Name':<30} {'Category':<15} {'Cuisine':<15} {'Ingredients':<50}")
    print("-" * 100)

    # Loop through each meal in the response data.
    for meal in meals:
        # Extract relevant details for each meal, handling missing fields.
        meal_id = int(meal.get("idMeal"))  # Unique ID for each meal.
        meal_name = meal.get("strMeal", "Unknown Meal")  # Meal name.
        category = meal.get("strCategory", "Unknown Category")  # Meal category.
        area = meal.get("strArea", "Unknown Area")  # Meal cuisine or origin.
        instructions = meal.get("strInstructions", "No instructions provided")  # Cooking instructions.

        # Collect ingredients (ingredients are in strIngredient1 to strIngredient20)
        ingredients = []
        for i in range(1, 21):
            ingredient = meal.get(f"strIngredient{i}")
            if ingredient:  # Add only non-empty ingredients
                ingredients.append(ingredient)
        ingredients_str = ", ".join(ingredients)  # Join all ingredients as a single string.

        # Print each processed row in the debug output.
        print(f"{meal_id:<10} {meal_name:<30} {category:<15} {area:<15} {ingredients_str:<50}")

        # Log fine-grained details for debugging.
        log.fine(f"Meal ID={meal_id}, meal_name={meal_name}")

        # Yield each meal as an upsert operation for Fivetran.
        yield op.upsert(
            table="meal",  # Table to which data is upserted.
            data={
                "meal_id": meal_id,
                "meal_name": meal_name,
                "category": category,
                "area": area,
                "instructions": instructions,
                "ingredients": ingredients_str,
            }
        )

    # Save the checkpoint state if needed (this API does not use a cursor-based sync).
    yield op.checkpoint(state={})  # Keep the state empty since this API doesn’t need a cursor.

# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode when executing the script directly.
if __name__ == "__main__":
    print("Running TheMealDB connector...")
    connector.debug()  # Run the connector in debug mode to simulate a Fivetran sync.
    print("Connector run complete.")
