# Fivetran_Connector_SDK
 ## Quickly build a custom US National Parks data connector using the Fivetran SDK

[Fivetran's Connector SDK](https://fivetran.com/docs/connectors/connector-sdk) allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources.

This is a simple example for how to work with the fivetran_connector_sdk module. 

It shows the use of a requirements.txt file and a connector that calls a publicly available API:

- US National Parks data from the National Park Service API

It also shows how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

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