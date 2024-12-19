#!/bin/bash

# Navigate to the test directory
cd /Users/kelly.kohlleffel/Documents/GitHub/Fivetran_Connector_SDK/examples/quick_start_examples/nationalparks/connector_tests

# Use the project's virtual environment
source ../../.venv/bin/activate

# Ensure required packages are installed
pip install requests fivetran-connector-sdk

# Run the test script
python test_connector_sync.py

# Optional: Deactivate virtual environment after running
deactivate

import sys
import os
import json
import logging

# Add the parent directory to Python path
sys.path.insert(0, os.path.abspath('..'))

# Import the connector from the test copy
from test_connector import connector, update, Logging, Operations

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def test_connector_sync():
    """
    Comprehensive test for National Parks Connector sync
    """
    try:
        # Load configuration from existing nps_config.json
        with open('../files/nps_config.json', 'r') as f:
            test_config = json.load(f)
        
        # Initialize empty state dictionary
        test_state = {}

        # Capture all operations from the update generator
        all_operations = []
        for op in update(test_config, test_state):
            # If the operation is a list, extend the operations
            if isinstance(op, list):
                all_operations.extend(op)
            else:
                all_operations.append(op)

        # Analyze operations
        upsert_operations = [op for op in all_operations if hasattr(op, 'type') and op.type == 'UPSERT']
        checkpoint_operations = [op for op in all_operations if hasattr(op, 'type') and op.type == 'CHECKPOINT']

        # Logging and Analysis
        logging.info("Sync Test Results:")
        logging.info(f"Total Operations: {len(all_operations)}")
        logging.info(f"Upsert Operations: {len(upsert_operations)}")
        logging.info(f"Checkpoint Operations: {len(checkpoint_operations)}")

        # Detailed Upsert Analysis
        table_upserts = {}
        for op in upsert_operations:
            if op.table not in table_upserts:
                table_upserts[op.table] = 0
            table_upserts[op.table] += 1

        logging.info("Upserts by Table:")
        for table, count in table_upserts.items():
            logging.info(f"{table}: {count} records")

        # Optional: Print first few upsert details
        if upsert_operations:
            logging.info("\nSample Upsert Details:")
            for op in upsert_operations[:3]:
                logging.info(f"Table: {op.table}")
                logging.info(f"Data: {op.data}")

    except Exception as e:
        logging.error(f"Sync Test Failed: {e}", exc_info=True)

if __name__ == "__main__":
    test_connector_sync()