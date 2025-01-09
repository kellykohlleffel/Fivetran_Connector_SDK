#!/bin/bash
echo "Starting debug process..."

# Clear Python cache
echo "Clearing Python cache..."
find . -type d -name "__pycache__" -exec rm -r {} +

# Run fivetran reset
echo "Running fivetran reset..."
fivetran reset

# Ensure files directory exists
echo "Ensuring files directory exists..."
mkdir -p files

# List contents of the files directory
echo "Contents of files directory:"
ls -la files/

# Run fivetran debug
echo "Running fivetran debug..."
fivetran debug

echo "Debug process complete."
