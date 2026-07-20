
# InLong Changelog

<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Be careful doing manual edits in this file. Do not change format
# of release header or remove the below marker. This file is generated.
# DO NOT REMOVE THIS MARKER; FOR INTERPOLATING CHANGES!-->

# Release InLong 2.4.0 - Released (as of 2026-07-20)

### Agent
|ISSUE|Summary|
|:--|:--|
|[INLONG-11950](https://github.com/apache/inlong/issues/11950)|[Improve][Agent] Missing case in switch statement|
|[INLONG-12143](https://github.com/apache/inlong/issues/12143)|[Improve][Agent] Fix Agent path traversal via unvalidated file source path|
|[INLONG-12148](https://github.com/apache/inlong/issues/12148)|[Improve][Agent] Replace /bin/sh -c with ProcessBuilder|

### Dashboard
|ISSUE|Summary|
|:--|:--|
|[INLONG-12044](https://github.com/apache/inlong/issues/12044)|[Bug][Dashboard] Agent installation method is missing|
|[INLONG-12052](https://github.com/apache/inlong/issues/12052)|[Improve][Dashboard] Optimization of text display in long forms on resource details page|

### Manager
|ISSUE|Summary|
|:--|:--|
|[INLONG-12071](https://github.com/apache/inlong/issues/12071)|[Bug][Manager] Querying MQ messages returned incorrect results from the LIKE matching operator|
|[INLONG-12073](https://github.com/apache/inlong/issues/12073)|[Bug][Manager] No proper error handling when MQ message query thread pool is exhausted|
|[INLONG-12075](https://github.com/apache/inlong/issues/12075)|[Improve][Manager] Optimize delete permission for InlongGroup: allow admin or in-charge|
|[INLONG-12079](https://github.com/apache/inlong/issues/12079)|[Improve][Manager] The AuditAlertRuleRequest lacks validation for orderField and orderType|
|[INLONG-12092](https://github.com/apache/inlong/issues/12092)|[Bug][Manager] Non-template responsible persons can view template information|
|[INLONG-12094](https://github.com/apache/inlong/issues/12094)|[Bug][Manager] Ordinary users can create new packages|
|[INLONG-12098](https://github.com/apache/inlong/issues/12098)|[Improve][Manager] Improve audit query nodeType mapping completeness and avoid null audit IDs|
|[INLONG-12100](https://github.com/apache/inlong/issues/12100)|[Improve][Manager] Fix incorrect spelling: "seperated" → "separated" in multiple files|
|[INLONG-12102](https://github.com/apache/inlong/issues/12102)|[Bug][Manager] Incorrect logic in input validation|
|[INLONG-12104](https://github.com/apache/inlong/issues/12104)|[Bug][Manager] There is a problem with the deletion process of the data flow group|
|[INLONG-12115](https://github.com/apache/inlong/issues/12115)|[Improve][Manager] Set the default value of openapi.auth.enabled to true|
|[INLONG-12129](https://github.com/apache/inlong/issues/12129)|[Improve][Manager] Fix the security vulnerability in /api/node/testConnection|
|[INLONG-12133](https://github.com/apache/inlong/issues/12133)|[Improve][Manager] Fix the security vulnerability in /api/cluster/testConnection|
|[INLONG-12135](https://github.com/apache/inlong/issues/12135)|[Improve][Manager] Enhance the verification against SQL injection|
|[INLONG-12144](https://github.com/apache/inlong/issues/12144)|[Improve][Manager] Perform permission verification for the deletion operation of streamsource|
|[INLONG-12149](https://github.com/apache/inlong/issues/12149)|[Improve][Manager] Add command whitelist validation in Manager|
|[INLONG-12153](https://github.com/apache/inlong/issues/12153)|[Improve][Manager] Add parameter validation for node in clusterController|
|[INLONG-12156](https://github.com/apache/inlong/issues/12156)|[Improve][Manager] Add verification of operator permissions during the creation and modification of tenant roles|
|[INLONG-12160](https://github.com/apache/inlong/issues/12160)|[Improve][Manager] Add permission verification when modifying the datanode|

### SDK
|ISSUE|Summary|
|:--|:--|
|[INLONG-12016](https://github.com/apache/inlong/issues/12016)|[Improve][SDK] Add Python Dataproxy SDK compile Dockerfile|
|[INLONG-12054](https://github.com/apache/inlong/issues/12054)|[Improve][SDK] Optimize InLong Dataproxy Python SDK dependency version requirements|
|[INLONG-12056](https://github.com/apache/inlong/issues/12056)|[Feature][SDK] TransformSDK supports the "not in" operator|
|[INLONG-12058](https://github.com/apache/inlong/issues/12058)|[Feature][SDK] Support retry mechanism when "server error" occurs in dataproxy client|
|[INLONG-12062](https://github.com/apache/inlong/issues/12062)|[Improve][SDK] Upgrade gnet version in dataproxy go sdk|
|[INLONG-12090](https://github.com/apache/inlong/issues/12090)|[Improve][SDK] Build standard Python Dataproxy SDK wheels based on PEP-517|
|[INLONG-12108](https://github.com/apache/inlong/issues/12108)|[Feature][SDK] TransformSDK supports decoding and transformation of ProtoBuf description files with multiple Proto files and multi-level NestedType|
|[INLONG-12111](https://github.com/apache/inlong/issues/12111)|[Feature][SDK] ProtoBuf decoding supports parsing map nodes|
|[INLONG-12113](https://github.com/apache/inlong/issues/12113)|[Feature][SDK] TransformSDK supports encoding of List, Struct, and Binary type fields for RowData|
|[INLONG-12117](https://github.com/apache/inlong/issues/12117)|[Feature][SDK] Support concat_struct/extract_struct/extract_binary function|
|[INLONG-12119](https://github.com/apache/inlong/issues/12119)|[Improve][SDK] Add connection timeout and tolerate partial endpoint failures during client initialization in DataProxy Go SDK|
|[INLONG-12120](https://github.com/apache/inlong/issues/12120)|[Improve][SDK] Optimize worker selection logic in DataProxy Go SDK|
|[INLONG-12121](https://github.com/apache/inlong/issues/12121)|[Improve][SDK] Log remote server address on send timeout and server errors in DataProxy Go SDK|
|[INLONG-12125](https://github.com/apache/inlong/issues/12125)|[Feature][SDK] Enhance Transform SDK protobuf processing and SQL alias parsing|
|[INLONG-12141](https://github.com/apache/inlong/issues/12141)|[Bug][SDK] PbSourceData returns protobuf default values instead of null for unset fields, causing incorrect data written to Iceberg/Parquet sinks|
|[INLONG-12158](https://github.com/apache/inlong/issues/12158)|[Feature][SDK] Support SQL reserved keywords as source field names (backtick-quoted columns)|

### Sort
|ISSUE|Summary|
|:--|:--|
|[INLONG-12065](https://github.com/apache/inlong/issues/12065)|[Improve][Sort] Sort Format supports outputting complete row information when field parsing errors occur|
|[INLONG-12139](https://github.com/apache/inlong/issues/12139)|[Bug][Sort] SortConfigUtil.checkUpdate throws NPE when DataFlowConfig.version is null, causing SortConfig reload to permanently stall|

### Audit
|ISSUE|Summary|
|:--|:--|
|[INLONG-12042](https://github.com/apache/inlong/issues/12042)|[Improve][Audit] Change the name of the audit item BSS to BIFANG|
|[INLONG-12046](https://github.com/apache/inlong/issues/12046)|[Bug][Audit] SQL script initialization failed|
|[INLONG-12060](https://github.com/apache/inlong/issues/12060)|[Improve][Audit] Optimize audit route management|
|[INLONG-12064](https://github.com/apache/inlong/issues/12064)|[Bug][Audit] Static ScheduledExecutorService in PulsarSink causes ClassLoader leaks and shared state corruption|
|[INLONG-12067](https://github.com/apache/inlong/issues/12067)|[Improve][Audit] Optimize the auditing data statistics by day|
|[INLONG-12069](https://github.com/apache/inlong/issues/12069)|[Improve][Audit] Fill with zero when there is no data for a given the API of day|
|[INLONG-12088](https://github.com/apache/inlong/issues/12088)|[Improve][Audit] Audit routing data source management supports domain names|
|[INLONG-12096](https://github.com/apache/inlong/issues/12096)|[Improve][Audit] Optimize function comments related to switching JDBC|
|[INLONG-12137](https://github.com/apache/inlong/issues/12137)|[Bug][Audit] Resolve thread-safety issues in the Audit SDK under high concurrency|

### Common
|ISSUE|Summary|
|:--|:--|
|[INLONG-12127](https://github.com/apache/inlong/issues/12127)|[Feature][Common] Add Json config|
|[INLONG-12131](https://github.com/apache/inlong/issues/12131)|[Improve][Common] Fix the issue where jsonconfig cannot be serialized correctly|

### CI
|ISSUE|Summary|
|:--|:--|
|[INLONG-12077](https://github.com/apache/inlong/issues/12077)|[Bug][CI] All unit tests execute failed|
|[INLONG-12081](https://github.com/apache/inlong/issues/12081)|[Improve][CI] Fix github action workflow policy violations|
|[INLONG-12083](https://github.com/apache/inlong/issues/12083)|[Improve][CI] Upgrade github actions version and specify permission of actions|
|[INLONG-12086](https://github.com/apache/inlong/issues/12086)|[Feature][CI] Upgrade parameters of first interaction|

### Docker
|ISSUE|Summary|
|:--|:--|
|[INLONG-12048](https://github.com/apache/inlong/issues/12048)|[Improve][Docker] openjdk:8-jdk is not accessible|
