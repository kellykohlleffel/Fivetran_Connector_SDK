# Fivetran_Connector_SDK
 ## Quickly build custom connectors using the Fivetran SDK

[Fivetran's Connector SDK](https://fivetran.com/docs/connectors/connector-sdk) allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources.

These are simple examples for how to work with the fivetran_connector_sdk module. 

They show the use of a requirements.txt file and a connector.py file that call publicly available APIs.

They also show how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

**APIs used in these examples**:

- Book data from the OpenLibrary API
- Meals data from TheMealsDB API
- US National Parks data from the US National Park Service API (extracts data from multiple tables)
- Solar system data from the Solar System OpenData API
- SpaceX launch data from the SpaceX API
- Weather forecast data from api.weather.gov for Cypress, TX, USA 

## Quick reference bash commands for running in the VS Code terminal

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
```
fivetran deploy --api-key <FIVETRAN-API-KEY> --destination <DESTINATION-NAME> --connection <CONNECTION-NAME>
```
## Alternatively: 

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
```
fivetran deploy --api-key <FIVETRAN-API-KEY> --destination <DESTINATION-NAME> --connection <CONNECTION-NAME>
```
## Fivetran Connector SDK in action

### Fivetran Connector SDK: Fivetran Sync Status

![Fivetran Sync Status Screenshot](./examples/quick_start_examples/weather/images/fivetran_syncstatus_kelly_cypress_weather_connector_sdk.png)

### Fivetran Connector SDK: Data moved with the Connector SDK to Snowflake

![Snowflake Snowsight Data Preview Screenshot](./examples/quick_start_examples/weather/images/snowflake_snowsight_datapreview_kelly_cypress_weather_connector_sdk.png)

### Fivetran Connector SDK: Snowflake Snowsight Dashboard with the new temperature data

![Snowflake Snowsight Dashboard Screenshot](./examples/quick_start_examples/weather/images/snowflake_snowsight_dashboard_fivetran_connector_sdk.png)