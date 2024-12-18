import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Any

import requests
from fivetran_sdk import Connector, Stream

class NPSConnector(Connector):
    def __init__(self):
        super().__init__()
        self.base_url = "https://developer.nps.gov/api/v1"
        self.config = self._load_config()
        self.session = self._init_session()
        self.state = self._load_state()

    def _load_config(self) -> Dict:
        """Load configuration from config.json file."""
        try:
            with open("config.json", "r") as f:
                config = json.load(f)
                return config["apis"]["nps"]
        except (FileNotFoundError, KeyError) as e:
            self.logger.error(f"Failed to load config: {str(e)}")
            raise

    def _load_state(self) -> Dict:
        """Load or initialize state from state.json file."""
        try:
            with open("state.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            # Initialize default state
            default_state = {
                "articles_last_sync": "1970-01-01T00:00:00Z",
                "alerts_last_sync": "1970-01-01T00:00:00Z",
                "parks_last_sync": "1970-01-01T00:00:00Z",
                "people_last_sync": "1970-01-01T00:00:00Z",
                "feespasses_last_sync": "1970-01-01T00:00:00Z"
            }
            with open("state.json", "w") as f:
                json.dump(default_state, f, indent=2)
            return default_state

    def _save_state(self):
        """Save current state to state.json file."""
        with open("state.json", "w") as f:
            json.dump(self.state, f, indent=2)

    def _update_sync_time(self, stream_name: str):
        """Update last sync time for a stream."""
        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.state[f"{stream_name}_last_sync"] = current_time
        self._save_state()

    def _init_session(self) -> requests.Session:
        """Initialize requests session with API key."""
        session = requests.Session()
        session.headers.update({
            "X-Api-Key": self.config["api_key"]
        })
        return session

    def streams(self) -> List[Stream]:
        """Define the available streams/tables."""
        return [
            Stream(
                name="parks",
                primary_key=["park_id"],
                schema={
                    "park_id": "STRING",
                    "name": "STRING",
                    "description": "STRING",
                    "state": "STRING",
                    "latitude": "FLOAT",
                    "longitude": "FLOAT",
                    "activities": "STRING",
                }
            ),
            Stream(
                name="articles",
                primary_key=["article_id"],
                schema={
                    "article_id": "STRING",
                    "title": "STRING",
                    "url": "STRING",
                    "related_parks": "STRING",
                    "park_names": "STRING",
                    "states": "STRING",
                    "listing_description": "STRING",
                    "date": "STRING",
                }
            ),
            Stream(
                name="feespasses",
                primary_key=["pass_id"],
                schema={
                    "pass_id": "STRING",
                    "park_id": "STRING",
                    "title": "STRING",
                    "cost": "FLOAT",
                    "description": "STRING",
                    "valid_for": "STRING",
                }
            ),
            Stream(
                name="people",
                primary_key=["person_id"],
                schema={
                    "person_id": "STRING",
                    "name": "STRING",
                    "title": "STRING",
                    "description": "STRING",
                    "url": "STRING",
                    "related_parks": "STRING",
                }
            ),
            Stream(
                name="alerts",
                primary_key=["alert_id"],
                schema={
                    "alert_id": "STRING",
                    "park_id": "STRING",
                    "title": "STRING",
                    "description": "STRING",
                    "category": "STRING",
                    "url": "STRING",
                }
            )
        ]

    def sync_parks(self, stream: Stream, state: Dict) -> List[Dict]:
        """Sync parks data."""
        try:
            response = self.session.get(f"{self.base_url}/parks", params={"limit": 50})
            response.raise_for_status()
            parks_data = response.json()["data"]
            
            results = [{
                "park_id": park["id"],
                "name": park["name"],
                "description": park["description"],
                "state": park["states"],
                "latitude": float(park["latitude"]) if park["latitude"] else None,
                "longitude": float(park["longitude"]) if park["longitude"] else None,
                "activities": json.dumps([act["name"] for act in park.get("activities", [])])
            } for park in parks_data]
            
            self._update_sync_time("parks")
            return results
        except Exception as e:
            self.logger.error(f"Error syncing parks: {str(e)}")
            raise

    def sync_articles(self, stream: Stream, state: Dict) -> List[Dict]:
        """Sync articles data with limit of 50 most recent."""
        try:
            params = {
                "limit": 50,
                "sort": "-dateCreated"  # Sort by most recent first
            }
            
            self.logger.info("Fetching 50 most recent articles")
            response = self.session.get(f"{self.base_url}/articles", params=params)
            response.raise_for_status()
            articles_data = response.json()["data"]

            results = [{
                "article_id": article["id"],
                "title": article["title"],
                "url": article["url"],
                "related_parks": json.dumps([park["id"] for park in article.get("relatedParks", [])]),
                "park_names": json.dumps([park["name"] for park in article.get("relatedParks", [])]),
                "states": json.dumps(article.get("states", [])),
                "listing_description": article.get("listingDescription", ""),
                "date": article.get("dateCreated", "")
            } for article in articles_data]
            
            self._update_sync_time("articles")
            return results
        except Exception as e:
            self.logger.error(f"Error syncing articles: {str(e)}")
            raise

    def sync_feespasses(self, stream: Stream, state: Dict) -> List[Dict]:
        """Sync fees and passes data."""
        try:
            response = self.session.get(f"{self.base_url}/feespasses")
            response.raise_for_status()
            fees_data = response.json()["data"]

            results = [{
                "pass_id": fee["id"],
                "park_id": fee.get("parkId", ""),
                "title": fee["title"],
                "cost": float(fee.get("cost", 0)),
                "description": fee.get("description", ""),
                "valid_for": fee.get("validFor", "")
            } for fee in fees_data]
            
            self._update_sync_time("feespasses")
            return results
        except Exception as e:
            self.logger.error(f"Error syncing fees and passes: {str(e)}")
            raise

    def sync_people(self, stream: Stream, state: Dict) -> List[Dict]:
        """Sync people data."""
        try:
            response = self.session.get(f"{self.base_url}/people")
            response.raise_for_status()
            people_data = response.json()["data"]

            results = [{
                "person_id": person["id"],
                "name": person["name"],
                "title": person.get("title", ""),
                "description": person.get("description", ""),
                "url": person.get("url", ""),
                "related_parks": json.dumps([park["id"] for park in person.get("relatedParks", [])])
            } for person in people_data]
            
            self._update_sync_time("people")
            return results
        except Exception as e:
            self.logger.error(f"Error syncing people: {str(e)}")
            raise

    def sync_alerts(self, stream: Stream, state: Dict) -> List[Dict]:
        """Sync alerts data with limit of 50 most recent."""
        try:
            params = {
                "limit": 50,
                "sort": "-id"  # Sort by most recent first (assuming IDs are sequential)
            }
            
            self.logger.info("Fetching 50 most recent alerts")
            response = self.session.get(f"{self.base_url}/alerts", params=params)
            response.raise_for_status()
            alerts_data = response.json()["data"]

            results = [{
                "alert_id": alert["id"],
                "park_id": alert.get("parkCode", ""),
                "title": alert["title"],
                "description": alert.get("description", ""),
                "category": alert.get("category", ""),
                "url": alert.get("url", "")
            } for alert in alerts_data]
            
            self._update_sync_time("alerts")
            return results
        except Exception as e:
            self.logger.error(f"Error syncing alerts: {str(e)}")
            raise

    def sync(self, stream: Stream, state: Dict) -> List[Dict]:
        """Main sync method that routes to appropriate sync function based on stream name."""
        sync_functions = {
            "parks": self.sync_parks,
            "articles": self.sync_articles,
            "feespasses": self.sync_feespasses,
            "people": self.sync_people,
            "alerts": self.sync_alerts
        }
        
        if stream.name not in sync_functions:
            raise ValueError(f"Unknown stream: {stream.name}")
            
        return sync_functions[stream.name](stream, state)

if __name__ == "__main__":
    connector = NPSConnector()
    connector.run()