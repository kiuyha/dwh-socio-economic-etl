#!/bin/bash

echo "Pulling latest changes from git..."
git pull

TARGET_DIR="${1:-../}"
DAG_FILE="socio_economic_etl_dag.py"

# Verify the file exists before attempting to move
if [ ! -f "$DAG_FILE" ]; then
    echo "Error: $DAG_FILE not found in the current directory."
    exit 1
fi

# Ensure the destination directory exists
mkdir -p "$TARGET_DIR"

mv "$DAG_FILE" "$TARGET_DIR/"

echo "Successfully moved $DAG_FILE to $TARGET_DIR/"