#!/bin/bash
echo "✅ Checking .venv2 Health..."
source ~/.venv2/bin/activate
echo "Python path: $(which python3)"
echo "Python version: $(python3 --version)"
echo "Installed SDK version:"
python3 -m pip list | grep fivetran_connector_sdk
