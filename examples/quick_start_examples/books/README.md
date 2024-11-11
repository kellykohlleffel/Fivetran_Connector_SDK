# Fivetran_Connector_SDK: Books Data
 ## Quickly build a custom OpenLibrary books data connector using the Fivetran SDK

[Fivetran's Connector SDK](https://fivetran.com/docs/connectors/connector-sdk) allows you to code a custom data connector using Python and deploy it as an extension of Fivetran. Fivetran automatically manages running the connector on your scheduled frequency and manages the required compute resources.

This is a simple example for how to work with the fivetran_connector_sdk module. 

It shows the use of a connector.py file that calls a publicly available API.

It also shows how to use the logging functionality provided by fivetran_connector_sdk, by logging important steps using log.info() and log.fine()

See the [Technical Reference documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update) and [Best Practices documentation](https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

## Book data from the OpenLibrary API

[OpenLibrary API](https://openlibrary.org/dev/docs/api/search)

This script connects to the OpenLibrary API using the Fivetran Connector SDK. It retrieves book information such as title, author, and publication date based on a search query, and stores the data in Fivetran using the SDK's upsert operation.

**Example usage**: This script can be used to demonstrate pulling book data from OpenLibrary, making it useful to better understand how the Fivetran Connector SDK works.

**Configuration**:
- A **search term** (e.g., "Python, SQL, History, etc.") can be provided in the configuration to customize the data retrieval and limit records.

## Quick reference bash commands for running in the VS Code terminal

### From this path: 
(.venv) kelly.kohlleffel@kelly Fivetran_Connector_SDK %

### Navigate to the quick_start_example/books
```
cd examples/quick_start_examples/books
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
### Navigate up one level from weather to books, for example
```
cd ../books
```
### Ensure the directory exists
```
mkdir -p files
```
### Activate your virtual environment
```
source .venv/bin/activate
```
### Navigate to the quick_start_example/books
```
cd examples/quick_start_examples/books
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

![Fivetran Sync Status Screenshot](./images/fivetran_syncstatus_books1_connector_sdk.png)

### Fivetran Connector SDK: Data moved with the Connector SDK to Snowflake

![Snowflake Snowsight Data Preview Screenshot](./images/snowflake_snowsight_datapreview2_books1_connector_sdk.png)

### Fivetran Connector SDK: Snowflake Snowsight Dashboard with the new books data

![Snowflake Snowsight Dashboard Screenshot](./images/snowflake_snowsight_dashboard_books1_connector_sdk.png)

### SQL query for all books (update the database and schema names)
```
SELECT * FROM HOL_DATABASE.BOOKS1_CONNECTOR_SDK.BOOK;
```

### SQL query for the books visualization (update the database and schema names)
```
SELECT 
    PUBLICATION_DATE, 
    COUNT(*) AS book_count
FROM 
    HOL_DATABASE.BOOKS1_CONNECTOR_SDK.BOOK
GROUP BY 
    PUBLICATION_DATE
ORDER BY 
    PUBLICATION_DATE;
```