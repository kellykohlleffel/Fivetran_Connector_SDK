"""
connector.py

This script connects to the National Park Service (NPS) API using the Fivetran Connector SDK.
It retrieves data on U.S. National Parks, articles, fees and passes, people, and alerts.
The data is stored in Fivetran using the SDK's upsert operation.

Example usage: This script demonstrates pulling park, article, feespasses, people, and alerts data from the NPS API, useful for 
analyzing park details, alerts, fees, passes, associated articles, and historical figures.

Configuration:
- An API key is required for accessing the NPS API. Replace 'YOUR_API_KEY' in the `API_KEY` variable
  with your actual API key.
- Set the `LIMIT` variable to control the number of records retrieved per table.

Requirements:
- No additional Python libraries are required, as `requests` and the 
  `fivetran_connector_sdk` are assumed to be pre-installed.
"""

import requests as rq  # Import requests for making HTTP requests, aliased as rq.
from fivetran_connector_sdk import Connector  # Connector class to set up the Fivetran connector.
from fivetran_connector_sdk import Logging as log  # Logging functionality to log key steps.
from fivetran_connector_sdk import Operations as op  # Operations class for Fivetran data operations.

# Set the API key and record retrieval limit once, and they will be used for all API requests.
API_KEY = "3qpAYIbdhT09TfPf9vf0MPoLrk9vrdK8Fn9BL0vt"  # Replace with your actual API key
LIMIT = 3  # Set the maximum number of records retrieved per table

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
            "table": "parks",
            "primary_key": ["park_id"],
            "columns": {
                "park_id": "STRING",
                "name": "STRING",
                "description": "STRING",
                "state": "STRING",
                "latitude": "FLOAT",
                "longitude": "FLOAT",
                "activities": "STRING",
            },
        },
        {
            "table": "articles",
            "primary_key": ["article_id"],
            "columns": {
                "article_id": "STRING",
                "title": "STRING",
                "url": "STRING",
                "related_parks": "STRING",
                "park_names": "STRING",
                "states": "STRING",
                "listing_description": "STRING",
                "date": "STRING",
            },
        },
        {
            "table": "feespasses",
            "primary_key": ["pass_id"],
            "columns": {
                "pass_id": "STRING",
                "park_id": "STRING",
                "title": "STRING",
                "cost": "FLOAT",
                "description": "STRING",
                "valid_for": "STRING",
            },
        },
        {
            "table": "people",
            "primary_key": ["person_id"],
            "columns": {
                "person_id": "STRING",
                "name": "STRING",
                "title": "STRING",
                "description": "STRING",
                "url": "STRING",
                "related_parks": "STRING",
            },
        },
        {
            "table": "alerts",
            "primary_key": ["alert_id"],
            "columns": {
                "alert_id": "STRING",
                "park_id": "STRING",
                "title": "STRING",
                "description": "STRING",
                "category": "STRING",
                "url": "STRING",
            },
        }
    ]

# Define the update function, which is called by Fivetran during each sync.
def update(configuration: dict, state: dict):
    """
    Retrieve data from the NPS API and send it to Fivetran.

    Args:
        configuration (dict): A dictionary containing configuration settings for the connector.
        state (dict): A dictionary containing the last sync state.
    
    Yields:
        op.upsert: An upsert operation for each record.
        op.checkpoint: A checkpoint operation to save the updated state.
    """
    
    # Fetch and yield parks data
    endpoint_parks = "https://developer.nps.gov/api/v1/parks"
    params_parks = {
        "api_key": API_KEY,
        "limit": LIMIT
    }
    
    response_parks = rq.get(endpoint_parks, params=params_parks)
    
    if response_parks.status_code == 200:
        data_parks = response_parks.json()
        parks = data_parks.get("data", [])
        log.info(f"Number of parks retrieved: {len(parks)}")

        for park in parks:
            park_id = park.get("id", "Unknown ID")
            name = park.get("fullName", "No Name")
            description = park.get("description", "No Description")
            state = ", ".join(park.get("states", []))
            latitude = park.get("latitude", None)
            longitude = park.get("longitude", None)
            activities = ", ".join(activity["name"] for activity in park.get("activities", []))

            yield op.upsert(
                table="parks",
                data={
                    "park_id": park_id,
                    "name": name,
                    "description": description,
                    "state": state,
                    "latitude": float(latitude) if latitude else None,
                    "longitude": float(longitude) if longitude else None,
                    "activities": activities,
                }
            )
    else:
        log.error(f"Parks API request failed with status code {response_parks.status_code}")

    # Fetch and yield articles data
    endpoint_articles = "https://developer.nps.gov/api/v1/articles"
    params_articles = {
        "api_key": API_KEY,
        "limit": LIMIT
    }
    
    response_articles = rq.get(endpoint_articles, params=params_articles)
    
    if response_articles.status_code == 200:
        data_articles = response_articles.json()
        articles = data_articles.get("data", [])
        log.info(f"Number of articles retrieved: {len(articles)}")

        for article in articles:
            article_id = article.get("id", "Unknown ID")
            title = article.get("title", "No Title")
            url = article.get("url", "")
            related_parks = [related_park.get("parkCode", "") for related_park in article.get("relatedParks", [])]
            park_names = [related_park.get("fullName", "") for related_park in article.get("relatedParks", [])]
            states = [related_park.get("states", "") for related_park in article.get("relatedParks", [])]
            listing_description = article.get("listingDescription", "")
            date = article.get("date", "")

            yield op.upsert(
                table="articles",
                data={
                    "article_id": article_id,
                    "title": title,
                    "url": url,
                    "related_parks": ", ".join(related_parks),
                    "park_names": ", ".join(park_names),
                    "states": ", ".join(states),
                    "listing_description": listing_description,
                    "date": date,
                }
            )
    else:
        log.error(f"Articles API request failed with status code {response_articles.status_code}")

    # Fetch and yield fees and passes data
    endpoint_feespasses = "https://developer.nps.gov/api/v1/parks"
    params_feespasses = {
        "api_key": API_KEY,
        "limit": LIMIT
    }

    response_feespasses = rq.get(endpoint_feespasses, params=params_feespasses)
    
    if response_feespasses.status_code == 200:
        data_feespasses = response_feespasses.json()
        parks = data_feespasses.get("data", [])
        log.info(f"Number of parks with fees and passes retrieved: {len(parks)}")

        for park in parks:
            park_id = park.get("id", "Unknown ID")
            fees = park.get("entranceFees", [])
            passes = park.get("entrancePasses", [])

            # Process entrance fees
            for fee in fees:
                yield op.upsert(
                    table="feespasses",
                    data={
                        "pass_id": fee.get("id", "Unknown ID"),
                        "park_id": park_id,
                        "title": fee.get("title", "No Title"),
                        "cost": float(fee.get("cost", 0)),
                        "description": fee.get("description", "No Description"),
                        "valid_for": fee.get("validFor", ""),
                    }
                )

            # Process entrance passes
            for pass_item in passes:
                yield op.upsert(
                    table="feespasses",
                    data={
                        "pass_id": pass_item.get("id", "Unknown ID"),
                        "park_id": park_id,
                        "title": pass_item.get("title", "No Title"),
                        "cost": float(pass_item.get("cost", 0)),
                        "description": pass_item.get("description", "No Description"),
                        "valid_for": pass_item.get("validFor", ""),
                    }
                )
    else:
        log.error(f"Fees and Passes API request failed with status code {response_feespasses.status_code}")

    # Fetch and yield people data
    endpoint_people = "https://developer.nps.gov/api/v1/people"
    params_people = {
        "api_key": API_KEY,
        "limit": LIMIT
    }

    response_people = rq.get(endpoint_people, params=params_people)
    
    if response_people.status_code == 200:
        data_people = response_people.json()
        people = data_people.get("data", [])
        log.info(f"Number of people retrieved: {len(people)}")

        for person in people:
            person_id = person.get("id", "Unknown ID")
            name = person.get("title", "No Name")
            title = person.get("listingDescription", "No Title")
            description = person.get("listingDescription", "No Description")
            url = person.get("url", "")
            related_parks = ", ".join(park["parkCode"] for park in person.get("relatedParks", []))

            yield op.upsert(
                table="people",
                data={
                    "person_id": person_id,
                    "name": name,
                    "title": title,
                    "description": description,
                    "url": url,
                    "related_parks": related_parks,
                }
            )
    else:
        log.error(f"People API request failed with status code {response_people.status_code}")

    # Fetch and yield alerts data
    endpoint_alerts = "https://developer.nps.gov/api/v1/alerts"
    params_alerts = {
        "api_key": API_KEY,
        "limit": LIMIT
    }

    response_alerts = rq.get(endpoint_alerts, params=params_alerts)
    
    if response_alerts.status_code == 200:
        data_alerts = response_alerts.json()
        alerts = data_alerts.get("data", [])
        log.info(f"Number of alerts retrieved: {len(alerts)}")

        for alert in alerts:
            alert_id = alert.get("id", "Unknown ID")
            park_id = alert.get("parkCode", "")
            title = alert.get("title", "No Title")
            description = alert.get("description", "No Description")
            category = alert.get("category", "No Category")
            url = alert.get("url", "")

            yield op.upsert(
                table="alerts",
                data={
                    "alert_id": alert_id,
                    "park_id": park_id,
                    "title": title,
                    "description": description,
                    "category": category,
                    "url": url,
                }
            )
    else:
        log.error(f"Alerts API request failed with status code {response_alerts.status_code}")

    # Save checkpoint state if needed (this API does not use a cursor-based sync).
    yield op.checkpoint(state={})

# Create the connector object for Fivetran.
connector = Connector(update=update, schema=schema)

# Run the connector in debug mode
if __name__ == "__main__":
    print("Running the NPS connector (Parks, Articles, FeesPasses, People, and Alerts tables)...")
    connector.debug()  # Run the connector in debug mode to simulate a Fivetran sync.
    print("Connector run complete.")
