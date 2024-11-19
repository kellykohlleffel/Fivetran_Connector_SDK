#!/bin/bash

# Read API key from config.json
API_KEY=$(jq -r '.fivetran.api_keys.MDS_DATABRICKS_HOL' config.json)

# Prompt for connection name
read -p "Enter connection name: " CONNECTION_NAME

# Set default if empty
if [ -z "$CONNECTION_NAME" ]; then
    CONNECTION_NAME="default-connection"
    echo "Using default connection name: $CONNECTION_NAME"
fi

fivetran deploy --api-key "$API_KEY" --destination MDS_DATABRICKS_HOL --connection "$CONNECTION_NAME"