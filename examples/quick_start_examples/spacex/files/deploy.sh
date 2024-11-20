#!/bin/bash

# Find config.json by searching up through parent directories
CONFIG_PATH=$(pwd)
while [[ "$CONFIG_PATH" != "/" ]]; do
    if [[ -f "$CONFIG_PATH/config.json" ]]; then
        break
    fi
    CONFIG_PATH=$(dirname "$CONFIG_PATH")
done

# Read API key from config.json
API_KEY=$(jq -r '.fivetran.api_keys.MDS_DATABRICKS_HOL' "$CONFIG_PATH/config.json")

# Prompt for destination name
read -p "Enter destination name: " DESTINATION_NAME

# Prompt for connection name
read -p "Enter connection name: " CONNECTION_NAME

# Set defaults if empty
if [ -z "$DESTINATION_NAME" ]; then
    DESTINATION_NAME="ADLS_UNITY_CATALOG"
    echo "Using default destination name: $DESTINATION_NAME"
fi

if [ -z "$CONNECTION_NAME" ]; then
    CONNECTION_NAME="default-connection"
    echo "Using default connection name: $CONNECTION_NAME"
fi

fivetran deploy --api-key "$API_KEY" --destination "$DESTINATION_NAME" --connection "$CONNECTION_NAME"