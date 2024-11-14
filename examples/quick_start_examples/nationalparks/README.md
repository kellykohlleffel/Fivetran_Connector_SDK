# Fivetran_Connector_SDK: US National Park Data
 ## Quickly build a custom US National Park data connector with multiple tables using the Fivetran Connector SDK

[Fivetran's Connector SDK](https://fivetran.com/docs/connectors/connector-sdk) allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources.

This is a simple example for how to work with the fivetran_connector_sdk module to extract data from multiple tables. 

It shows the use of a connector.py file that calls a publicly available API.

It also shows how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

## US National Park data from the National Park Service API

[US National Park Service API](https://www.nps.gov/subjects/developer/api-documentation.htm)

This script connects to the National Park Service (NPS) API using the Fivetran Connector SDK. It retrieves data from **multiple tables** on U.S. national parks, articles, fees and passes, people, and alerts. The data is stored in Fivetran using the SDK's upsert operation.

**Example usage**: This script demonstrates pulling data from multiple tables including park, article, feespasses, people, and alerts data from the NPS API, useful for analyzing park details, alerts, fees, passes, associated articles, and historical figures.

**Configuration**:
- An API key is required for accessing the NPS API. Replace 'YOUR_API_KEY' in the `API_KEY` variable
  with your actual API key.
- Set the `LIMIT` variable to control the number of records retrieved per table.

## Quick reference bash commands for running in the VS Code terminal

### From this path: 
(.venv) kelly.kohlleffel@kelly Fivetran_Connector_SDK %

### Navigate to the quick_start_example/nationalparks
```
cd examples/quick_start_examples/nationalparks
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

### Navigate to the Fivetran_Connector_SDK directory in Documents/Github
```
cd ~/Documents/Github/Fivetran_Connector_SDK
```
### Navigate up one level from weather to nationalparks, for example
```
cd ../nationalparks
```
### Ensure the directory exists
```
mkdir -p files
```
### Activate your virtual environment
```
source .venv/bin/activate
```
### Navigate to the quick_start_example/nationalparks
```
cd examples/quick_start_examples/nationalparks
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

![Fivetran Sync Status Screenshot](./images/fivetran_syncstatus_nationalparks_connector_sdk.png)

### Fivetran Connector SDK: Data moved with the Connector SDK to Snowflake

![Snowflake Snowsight Data Preview Screenshot](./images/snowflake_snowsight_datapreview_nationalparks_connector_sdk.png)

### Fivetran Connector SDK: Snowflake Snowsight Dashboard with the new National Parks data

![Snowflake Snowsight Dashboard Screenshot](./images/snowflake_snowsight_dashboard_nationalparks_connector_sdk.png)

### SQL queries for National Parks data (update the database and schema names)

### Count Number of Activities by Park Name
```
-- Step 1: Select activity data for each park and split the activities into an array
WITH activity_counts AS (
    SELECT 
        NAME AS PARK_NAME,
        SPLIT(ACTIVITIES, ', ') AS ACTIVITY_ARRAY
    FROM HOL_DATABASE.NATIONALPARKS7_CONNECTOR_SDK.parks
    WHERE ARRAY_SIZE(SPLIT(ACTIVITIES, ', ')) > 0
),
-- Step 2: Flatten the activity array into individual rows
expanded_activities AS (
    SELECT 
        PARK_NAME,
        ACTIVITY.VALUE::STRING AS ACTIVITY
    FROM activity_counts,
    LATERAL FLATTEN(INPUT => ACTIVITY_ARRAY) AS ACTIVITY
)

-- Step 3: Aggregate the count of each activity by park
SELECT 
    PARK_NAME,
    ACTIVITY,
    COUNT(*) AS ACTIVITY_COUNT
FROM expanded_activities
GROUP BY PARK_NAME, ACTIVITY
ORDER BY PARK_NAME, ACTIVITY_COUNT DESC;
```
