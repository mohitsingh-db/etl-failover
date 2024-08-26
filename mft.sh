#!/bin/bash

# Input parameters
storageaccname="westus2drstorageacct"
storageaccname2="eastus2drstorageacct"
container1="external"
container2="external"

# Get the account keys for both storage accounts
storage_key1=$(az storage account keys list --account-name "${storageaccname}" --query '[0].value' --output tsv)
storage_key2=$(az storage account keys list --account-name "${storageaccname2}" --query '[0].value' --output tsv)

# Generate a human-readable timestamp
timestamp=$(date +"%Y%m%d_%H%M%S")

# Define the file name with the timestamp
file_name="sample_file_${timestamp}.txt"

# Define the paths (replace with your actual paths if needed)
path1="${container1}/data/ingestion/${file_name}"
path2="${container2}/data/ingestion/${file_name}"

# Sample content to be written to the files
content="Sample content created at ${timestamp}"

# Create a temporary file to hold the content
temp_file=$(mktemp)
echo "${content}" > "${temp_file}"

# Function to handle upload with error checking
upload_file() {
  local storage_account=$1
  local storage_key=$2
  local container_name=$3
  local file_path=$4

  echo "Uploading to ${storage_account}..."

  az storage blob upload \
    --account-name "${storage_account}" \
    --account-key "${storage_key}" \
    --container-name "${container_name}" \
    --file "${temp_file}" \
    --name "${file_path}"

  # Check if the upload was successful
  if [ $? -eq 0 ]; then
    echo "File successfully uploaded to abfss://${container_name}@${storage_account}.dfs.core.windows.net/${file_path}"
  else
    echo "Failed to upload to ${storage_account}. Skipping..."
  fi
}

# Upload to Central US (path1)
upload_file "${storageaccname}" "${storage_key1}" "${container1}" "data/ingestion/${file_name}"

# Upload to East US 2 (path2)
upload_file "${storageaccname2}" "${storage_key2}" "${container2}" "data/ingestion/${file_name}"

# Cleanup the temporary file
rm -f "${temp_file}"

echo "Script completed."
