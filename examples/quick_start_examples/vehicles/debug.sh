#!/bin/bash
echo "Starting debug process..."

echo "Clearing Python cache..."
find . -type d -name "__pycache__" -exec rm -r {} +

echo "Running fivetran reset..."
fivetran reset

echo "Creating files directory..."
mkdir -p files

echo "Contents of files directory:"
ls -la files/

echo "Running fivetran debug..."
fivetran debug