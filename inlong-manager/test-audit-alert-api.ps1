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

# InLong Audit Alert Rule API Test Script for PowerShell
# This script is used to test the REST API for audit alert rules

$baseUrl = "http://localhost:8080/api"
$headers = @{
    "Content-Type" = "application/json"
}

Write-Host "=== InLong Audit Alert Rule API Test ===" -ForegroundColor Green

# 1. Create alert rule
Write-Host "`n1. Creating alert rule..." -ForegroundColor Yellow
$createBody = @{
    inlongGroupId = "test_group_001"
    inlongStreamId = "test_stream_001"
    auditId = "3"
    alertName = "Data Loss Alert"
    condition = "count < 1000 OR delay > 60000"
    level = "ERROR"
    notifyType = "EMAIL"
    receivers = "admin@example.com,ops@example.com"
    enabled = $true
} | ConvertTo-Json

try {
    $createResponse = Invoke-RestMethod -Uri "$baseUrl/audit/alert/rule" -Method POST -Headers $headers -Body $createBody
    Write-Host "Create Response:" -ForegroundColor Cyan
    $createResponse | ConvertTo-Json -Depth 3
    $ruleId = $createResponse.data.id
    Write-Host "Created Rule ID: $ruleId" -ForegroundColor Green
} catch {
    Write-Host "Create Failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# 2. Query single alert rule
Write-Host "`n2. Querying single alert rule..." -ForegroundColor Yellow
try {
    $getResponse = Invoke-RestMethod -Uri "$baseUrl/audit/alert/rule/$ruleId" -Method GET -Headers $headers
    Write-Host "Query Response:" -ForegroundColor Cyan
    $getResponse | ConvertTo-Json -Depth 3
} catch {
    Write-Host "Query Failed: $($_.Exception.Message)" -ForegroundColor Red
}

# 3. Update alert rule
Write-Host "`n3. Updating alert rule..." -ForegroundColor Yellow
$updateBody = @{
    id = $ruleId
    inlongGroupId = "test_group_001"
    inlongStreamId = "test_stream_001"
    auditId = "3"
    alertName = "Data Loss Alert - Updated"
    condition = "count < 500 OR delay > 30000"
    level = "CRITICAL"
    notifyType = "SMS"
    receivers = "admin@example.com"
    enabled = $false
} | ConvertTo-Json

try {
    $updateResponse = Invoke-RestMethod -Uri "$baseUrl/audit/alert/rule" -Method PUT -Headers $headers -Body $updateBody
    Write-Host "Update Response:" -ForegroundColor Cyan
    $updateResponse | ConvertTo-Json -Depth 3
} catch {
    Write-Host "Update Failed: $($_.Exception.Message)" -ForegroundColor Red
}

# 4. Batch query alert rules
Write-Host "`n4. Batch querying alert rules..." -ForegroundColor Yellow
try {
    $listResponse = Invoke-RestMethod -Uri "$baseUrl/audit/alert/rule/list?inlongGroupId=test_group_001" -Method GET -Headers $headers
    Write-Host "Batch Query Response:" -ForegroundColor Cyan
    $listResponse | ConvertTo-Json -Depth 3
} catch {
    Write-Host "Batch Query Failed: $($_.Exception.Message)" -ForegroundColor Red
}

# 5. Query all enabled alert rules
Write-Host "`n5. Querying all enabled alert rules..." -ForegroundColor Yellow
try {
    $enabledResponse = Invoke-RestMethod -Uri "$baseUrl/audit/alert/rule/enabled" -Method GET -Headers $headers
    Write-Host "Enabled Rules Query Response:" -ForegroundColor Cyan
    $enabledResponse | ConvertTo-Json -Depth 3
} catch {
    Write-Host "Enabled Rules Query Failed: $($_.Exception.Message)" -ForegroundColor Red
}

# 6. Delete alert rule
Write-Host "`n6. Deleting alert rule..." -ForegroundColor Yellow
try {
    $deleteResponse = Invoke-RestMethod -Uri "$baseUrl/audit/alert/rule/$ruleId" -Method DELETE -Headers $headers
    Write-Host "Delete Response:" -ForegroundColor Cyan
    $deleteResponse | ConvertTo-Json -Depth 3
} catch {
    Write-Host "Delete Failed: $($_.Exception.Message)" -ForegroundColor Red
}

# 7. Verify deletion (should return 404)
Write-Host "`n7. Verifying deletion result..." -ForegroundColor Yellow
try {
    $verifyResponse = Invoke-RestMethod -Uri "$baseUrl/audit/alert/rule/$ruleId" -Method GET -Headers $headers
    Write-Host "Verification Response:" -ForegroundColor Cyan
    $verifyResponse | ConvertTo-Json -Depth 3
} catch {
    Write-Host "Verification successful - Rule no longer exists: $($_.Exception.Message)" -ForegroundColor Green
}

Write-Host "`n=== API Test Completed ===" -ForegroundColor Green