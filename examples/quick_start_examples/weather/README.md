# Fivetran_Connector_SDK: Weather Data
 ## Quickly build a custom weather data connector using the Fivetran Connector SDK

[Fivetran's Connector SDK](https://fivetran.com/docs/connectors/connector-sdk) allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources.

This is a simple example for how to work with the fivetran_connector_sdk module. 

It shows the use of a connector.py file that calls a publicly available API.

It also shows how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

## Weather data from the National Weather Service API

[National Weather Service API](https://www.weather.gov/documentation/services-web-api)

This script retrieves weather forecast data for **Cypress, Texas**, including weather forecast period names, start and end times, and temperature. The data is stored in Fivetran using the SDK's upsert operation.

**Example usage**: This script demonstrates how to pull weather forecast data from a public API, providing a straightforward example of using the Fivetran Connector SDK for syncing data.

## Quick reference bash commands for running in your IDE (e.g. VS Code terminal)

### From this path: 
(.venv) kelly.kohlleffel@kelly Fivetran_Connector_SDK %

### Navigate to the quick_start_example/weather
```
cd examples/quick_start_examples/weather
```
### Run the custom connector code
```
python connector.py
```
### Deploy the connector to Fivetran

### Make the deploy.sh file executable and deploy to Fivetran. While deploying, prompts are used for the Fivetran Account Name, the Fivetran Destination Name and a unique Fivetran Connector Name for this individual connector.

* You will be prompted for the **Fivetran Account Name** **Fivetran Destination Name** and a unique **Fivetran Connector Name**

```
chmod +x files/deploy.sh
./files/deploy.sh
```

### For reference, this is the Fivetran deployment script that runs in the deploy.sh file when executed.
```
fivetran deploy --api-key <FIVETRAN-API-KEY> --destination <DESTINATION-NAME> --connection <CONNECTION-NAME>
```

## Alternatively: 

### Navigate to the Fivetran_Connector_SDK directory in Documents/Github
```
cd ~/Documents/Github/Fivetran_Connector_SDK
```
### Navigate up one level from books to weather, for example
```
cd ../weather
```
### Ensure the directory exists
```
mkdir -p files
```
### Activate your virtual environment
```
source .venv/bin/activate
```
### Navigate to the quick_start_example/weather
```
cd examples/quick_start_examples/weather
```
### Install the Fivetran requirements.txt file
```
pip install -r requirements.txt
```
### Run the custom connector code
```
python connector.py
```
### Deploy the connector to Fivetran

This repo uses a **deploy.sh** file to prompt for the following:
* Fivetran Account Name (this references an API key in the config.json file that is associated with the Fivetran Account Name input)
* Fivetran Destination Name
* Fivetran Connector Name

For demo purposes, there is a default Fivetran account (in brackets) and default Fivetran destination. Simply clicking ENTER will use those defaults. A Fivetran connector name is required.

* You will be prompted for the **Fivetran Account Name** **Fivetran Destination Name** and a unique **Fivetran Connector Name**

```
chmod +x files/deploy.sh
./files/deploy.sh
```

### For reference, this is the Fivetran deployment script that runs in the deploy.sh file when executed.
```
fivetran deploy --api-key <FIVETRAN-API-KEY> --destination <DESTINATION-NAME> --connection <CONNECTION-NAME>
```
## Fivetran Connector SDK in action

### Fivetran Connector SDK: Fivetran Sync Status

![Fivetran Sync Status Screenshot](./images/fivetran_syncstatus_kelly_cypress_weather_connector_sdk.png)

### Fivetran Connector SDK: Data moved with the Connector SDK to Snowflake

![Snowflake Snowsight Data Preview Screenshot](./images/snowflake_snowsight_datapreview_kelly_cypress_weather_connector_sdk.png)

### Fivetran Connector SDK: Snowflake Snowsight Dashboard with the new temperature data

![Snowflake Snowsight Dashboard Screenshot](./images/snowflake_snowsight_dashboard_fivetran_connector_sdk.png)

### SQL query for Cypress, TX weather (update the database and schema names)
```
SELECT * FROM HOL_DATABASE.WEATHER_CONNECTOR_SDK.PERIOD;
```
