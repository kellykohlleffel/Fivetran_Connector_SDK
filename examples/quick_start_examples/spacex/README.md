# Fivetran_Connector_SDK: SpaceX Launch Data
 ## Quickly build a custom SpaceX data connector using the Fivetran Connector SDK

[Fivetran's Connector SDK](https://fivetran.com/docs/connectors/connector-sdk) allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources.

This is a simple example for how to work with the fivetran_connector_sdk module. 

It shows the use of a connector.py file that calls a publicly available API.

It also shows how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

## SpaceX launch data from the SpaceX API

[SpaceX API](https://github.com/r-spacex/SpaceX-API/tree/master/docs#rspacex-api-docs)

This script connects to the SpaceX API using the Fivetran Connector SDK. It retrieves information about past SpaceX launches, including mission name, launch date, rocket type, and launch site, and stores this data in Fivetran using the SDK's upsert operation.

**Example usage**: This script can be used to demonstrate pulling launch data from SpaceX, making it useful for showcasing how the Fivetran Connector SDK works.

## Quick reference bash commands for running in the VS Code terminal

### From this path: 
(.venv) kelly.kohlleffel@kelly Fivetran_Connector_SDK %

### Navigate to the quick_start_example/spacex
```
cd examples/quick_start_examples/spacex
```
### Run the custom connector code
```
python connector.py
```

### NEW! Make the deploy.sh file executable and deploy to Fivetran with prompts for Destination Name and Connector Name

* You will be prompted for the **Destination Name** and the **Connector Name**

* Currently the code is only using a single Fivetran account for testing purposes - the account is **MDS_DATABRICKS_HOL**

```
chmod +x files/deploy.sh
./files/deploy.sh
```

### OLD: Deploy the connector to Fivetran
```
fivetran deploy --api-key <FIVETRAN-API-KEY> --destination <DESTINATION-NAME> --connection <CONNECTION-NAME>
```
## Alternatively: 

### Navigate to the Fivetran_Connector_SDK directory in Documents/Github
```
cd ~/Documents/Github/Fivetran_Connector_SDK
```
### Navigate up one level from weather to spacex, for example
```
cd ../spacex
```
### Ensure the directory exists
```
mkdir -p files
```
### Activate your virtual environment
```
source .venv/bin/activate
```
### Navigate to the quick_start_example/spacex
```
cd examples/quick_start_examples/spacex
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

![Fivetran Sync Status Screenshot](./images/fivetran_syncstatus_spacex_connector_sdk.png)

### Fivetran Connector SDK: Data moved with the Connector SDK to Snowflake

![Snowflake Snowsight Data Preview Screenshot](./images/snowflake_snowsight_datapreview1_spacex_connector_sdk.png)

### Fivetran Connector SDK: Snowflake Snowsight Dashboard with the new SpaceX data

![Snowflake Snowsight Dashboard Screenshot](./images/snowflake_snowsight_dashboard1_spacex_connector_sdk.png)

### SQL query for all SpaceX launches (update the database and schema names)
```
SELECT * FROM HOL_DATABASE.SPACEX_CONNECTOR_SDK.LAUNCH;
```
