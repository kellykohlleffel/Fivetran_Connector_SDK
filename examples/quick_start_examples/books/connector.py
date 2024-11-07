"""
connector.py

This script connects to the OpenLibrary API using the Fivetran Connector SDK. 
It retrieves book information such as title, author, and publication date based 
on a search query, and stores the data in Fivetran using the SDK's upsert operation.

Example usage: This script can be used to demonstrate pulling book data from 
OpenLibrary, making it useful for educational or demo purposes.

Configuration:
- The search term (e.g., "Python") can be provided in the configuration to 
  customize the data retrieval.

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
    """
    return [
        {
            "table": "book",  # Table name in the destination.
            "primary_key": ["title"],  # Primary key column for deduplication.
            "columns": {  # Columns and their data types.
                "title": "STRING",  # Book title as a string.
                "author": "STRING",  # Author(s) as a string.
                "publication_date": "DATE",  # First publication date as a date.
            },
        }
    ]

# Define the update function, which is called by Fivetran during each sync.
def update(configuration: dict, state: dict):
    """
    Retrieve data from the OpenLibrary API and send it to Fivetran.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
        state (dict): A dictionary containing the last sync state, such as cursor values.
    
    Yields:
        op.upsert: An upsert operation for each book record.
        op.checkpoint: A checkpoint operation to save the updated state.
    """
    # Set the search term from the configuration or default to 'Python'.
    search_query = configuration.get("search_query", "Python")  # Set the search term.
    cursor = state.get("publication_date", "0001-01-01")  # Set the cursor for pagination.

    # Fetch data from OpenLibrary API.
    response = rq.get(f"https://openlibrary.org/search.json?q={search_query}")
    data = response.json()
    books = data.get('docs', [])  # Retrieve the list of books from the response.
    log.info(f"Number of books retrieved: {len(books)}")

    # Print table header for visual output in debug mode.
    print("\n--- Processing and Printing Synced Data ---")
    print(f"{'Title':<40} {'Author':<25} {'Publication Date':<15}")
    print("-" * 80)

    # Loop through each book in the response data.
    for book in books:
        # Extract relevant details for each book, handling missing fields.
        title = book.get("title", "Unknown Title")
        author = ", ".join(book.get("author_name", ["Unknown Author"]))
        publication_date = book.get("first_publish_year", None)

        # Skip entries with no publication date or if the date is older than the cursor.
        if publication_date is None or publication_date < int(cursor):
            continue

        # Print each processed row in the debug output.
        print(f"{title:<40} {author:<25} {publication_date:<15}")

        # Log fine-grained details for debugging.
        log.fine(f"Book title={title}, author(s)={author}")

        # Yield each book as an upsert operation for Fivetran.
        yield op.upsert(
            table="book",
            data={
                "title": title,
                "author": author,
                "publication_date": str(publication_date)
            }
        )

        # Update the cursor to the latest publication date encountered.
        cursor = max(cursor, publication_date)

    # Save the updated state with the latest publication date.
    yield op.checkpoint(state={"publication_date": cursor})

# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode when executing the script directly.
if __name__ == "__main__":
    print("Running the OpenLibrary connector...")
    connector.debug()  # Run the connector in debug mode to simulate a Fivetran sync.
    print("Connector run complete.")
