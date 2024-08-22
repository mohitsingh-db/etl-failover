#!/bin/bash

# Variables
databricks_instance=$1       # Databricks instance URL
scope_name=$2                # Secret scope name
secret_key=$3                # Secret key
pat_token=$4                 # Databricks Personal Access Token

# Create Secret Scope
echo "Creating secret scope: $scope_name"
create_scope_command="curl -X POST '${databricks_instance}/api/2.0/secrets/scopes/create' \
  -H 'Authorization: Bearer $pat_token' \
  -H 'Content-Type: application/json' \
  -d '{
       \"scope\": \"$scope_name\",
       \"scope_backend_type\": \"DATABRICKS\"
   }'"
echo "Running command: $create_scope_command"
eval $create_scope_command

# Add Secret to Scope
echo "Adding secret '$secret_key' to scope '$scope_name'"
add_secret_command="curl -X POST '${databricks_instance}/api/2.0/secrets/put' \
  -H 'Authorization: Bearer $pat_token' \
  -H 'Content-Type: application/json' \
  -d '{
       \"scope\": \"$scope_name\",
       \"key\": \"$secret_key\",
       \"string_value\": \"$pat_token\"
   }'"
echo "Running command: $add_secret_command"
eval $add_secret_command

# Confirm that the secret exists by listing all secrets in the scope
echo "Verifying the secret exists in scope '$scope_name'"
list_secrets_command="curl -s -X GET '${databricks_instance}/api/2.0/secrets/list?scope=${scope_name}' \
  -H 'Authorization: Bearer $pat_token'"
echo "Running command: $list_secrets_command"
list_secrets_response=$(eval $list_secrets_command)

# Check if the secret exists
if echo "$list_secrets_response" | grep -q "\"key\": \"$secret_key\""; then
  echo "Secret '$secret_key' exists in scope '$scope_name'."
else
  echo "Secret '$secret_key' does not exist in scope '$scope_name'."
fi

# Optional: Print the full list of secrets for confirmation
echo "List of secrets in scope '$scope_name':"
echo "$list_secrets_response"
