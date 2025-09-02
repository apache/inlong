#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# InLong Audit Alert Rule API Test Script
# This script is used to test the REST API for audit alert rules

BASE_URL="http://localhost:8083/inlong/manager/api"
CONTENT_TYPE="Content-Type: application/json"

echo "=== InLong Audit Alert Rule API Test ==="

# 1. Create alert rule
echo "1. Creating alert rule..."
CREATE_RESPONSE=$(curl -s -X POST "$BASE_URL/audit/alert/rule" \
  -H "$CONTENT_TYPE" \
  -d '{
    "inlongGroupId": "test_group_001",
    "inlongStreamId": "test_stream_001", 
    "auditId": "3",
    "alertName": "Data Loss Alert",
    "condition": "count < 1000 OR delay > 60000",
    "level": "ERROR",
    "notifyType": "EMAIL",
    "receivers": "admin@example.com,ops@example.com",
    "enabled": true
  }')

echo "Create Response: $CREATE_RESPONSE"
RULE_ID=$(echo $CREATE_RESPONSE | grep -o '"id":[0-9]*' | grep -o '[0-9]*')
echo "Created Rule ID: $RULE_ID"

# 2. Query single alert rule
echo ""
echo "2. Querying single alert rule..."
curl -s -X GET "$BASE_URL/audit/alert/rule/$RULE_ID" \
  -H "$CONTENT_TYPE" | jq '.'

# 3. Update alert rule
echo ""
echo "3. Updating alert rule..."
UPDATE_RESPONSE=$(curl -s -X PUT "$BASE_URL/audit/alert/rule" \
  -H "$CONTENT_TYPE" \
  -d "{
    \"id\": $RULE_ID,
    \"inlongGroupId\": \"test_group_001\",
    \"inlongStreamId\": \"test_stream_001\",
    \"auditId\": \"3\",
    \"alertName\": \"Data Loss Alert - Updated\",
    \"condition\": \"count < 500 OR delay > 30000\",
    \"level\": \"CRITICAL\",
    \"notifyType\": \"SMS\",
    \"receivers\": \"admin@example.com\",
    \"enabled\": false
  }")

echo "Update Response: $UPDATE_RESPONSE"

# 4. Batch query alert rules
echo ""
echo "4. Batch querying alert rules..."
curl -s -X GET "$BASE_URL/audit/alert/rule/list?inlongGroupId=test_group_001" \
  -H "$CONTENT_TYPE" | jq '.'

# 5. Query all enabled alert rules
echo ""
echo "5. Querying all enabled alert rules..."
curl -s -X GET "$BASE_URL/audit/alert/rule/enabled" \
  -H "$CONTENT_TYPE" | jq '.'

# 6. Delete alert rule
echo ""
echo "6. Deleting alert rule..."
DELETE_RESPONSE=$(curl -s -X DELETE "$BASE_URL/audit/alert/rule/$RULE_ID" \
  -H "$CONTENT_TYPE")

echo "Delete Response: $DELETE_RESPONSE"

# 7. Verify deletion (should return 404)
echo ""
echo "7. Verifying deletion result..."
curl -s -X GET "$BASE_URL/audit/alert/rule/$RULE_ID" \
  -H "$CONTENT_TYPE" | jq '.'

echo ""
echo "=== API Test Completed ==="