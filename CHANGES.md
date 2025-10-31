
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

# Release InLong 2.3.0 - Released (as of 2025-10-31)
### Agent
|ISSUE|Summary|
|:--|:--|
|[INLONG-11995](https://github.com/apache/inlong/issues/11995)|[Feature][Agent] The Agent supports parallel creation of Sender connections to the DataProxy, improving creation efficiency|
|[INLONG-12003](https://github.com/apache/inlong/issues/12003)|[Feature][Agent] Added the agent_ext.properties configuration file loading to prevent personalized configurations from being lost when the agent.properties configuration file is overwritten during upgrades|

### Dashboard
|ISSUE|Summary|
|:--|:--|
|[INLONG-11972](https://github.com/apache/inlong/issues/11972)|[Bug][Dashboard] Occasional audit queries based on group queries have query conditions including streamid|
|[INLONG-11897](https://github.com/apache/inlong/issues/11897)|[Bug][Dashboard] Click on the audit page to query multiple calls to the interface|
|[INLONG-11894](https://github.com/apache/inlong/issues/11894)|[Feature][Dashboard] Added group and stream switching on the audit page|

### Manager
|ISSUE|Summary|
|:--|:--|
|[INLONG-11997](https://github.com/apache/inlong/issues/11997)|[Feature][Manager] Add comprehensive audit alert rule management API|
|[INLONG-11984](https://github.com/apache/inlong/issues/11984)|[Bug][Manager] Hive table creation statement error|
|[INLONG-11969](https://github.com/apache/inlong/issues/11969)|[Bug][Manager] org.apache.flink.client.program.ProgramInvocationException|
|[INLONG-11891](https://github.com/apache/inlong/issues/11891)|[Improve][Manager]Support querying topic audit information|
|[INLONG-11881](https://github.com/apache/inlong/issues/11881)|[Feature][Manager] Sink source field supports configuring the fuction function|
|[INLONG-11879](https://github.com/apache/inlong/issues/11879)|[Improve][Manager] Support parsing transform configuration as transform SQL|
|[INLONG-11877](https://github.com/apache/inlong/issues/11877)|[Improve][Manager] Support verifying transform SQL|
|[INLONG-11875](https://github.com/apache/inlong/issues/11875)|[Bug][Manager] InLong standalone mode deployment failed|

### SDK
|ISSUE|Summary|
|:--|:--|
|[INLONG-12019](https://github.com/apache/inlong/issues/12019)|[Feature][SDK] Transformation supports a caching mechanism for processing identical function parameters within the same SQL statement to reduce redundant computations|
|[INLONG-12014](https://github.com/apache/inlong/issues/12014)|[Improve][SDK] Add C++ Dataproxy SDK compile Dockerfile|
|[INLONG-12012](https://github.com/apache/inlong/issues/12012)|[Improve][SDK] Change the Python Dataproxy SDK user-defined installation path configuration method|
|[INLONG-12010](https://github.com/apache/inlong/issues/12010)|[Bug][SDK] Python DataProxy SDK reported data is truncated|
|[INLONG-11974](https://github.com/apache/inlong/issues/11974)|[Feature][Sort] Upgrade pulsar sdk version to 4.0.3|
|[INLONG-11976](https://github.com/apache/inlong/issues/11976)|[Improve][SDK] DataProxy Go SDK support detailed error information in server responses|
|[INLONG-11961](https://github.com/apache/inlong/issues/11961)|[Feature][SDK] Transform supports array index access, the WHERE clause supports the LIKE operator, and the str_to_json function converts KV-format data into JSON format|
|[INLONG-11954](https://github.com/apache/inlong/issues/11954)|[Bug][SDK] Potential data race in Golang SDK|
|[INLONG-11952](https://github.com/apache/inlong/issues/11952)|[Feature][SDK] Transform handles field-level exceptions by nullifying only the affected field while preserving the entire record|
|[INLONG-11926](https://github.com/apache/inlong/issues/11926)|[Feature][SDK] If SortSDK fails to retrieve GroupId and StreamId from the InLongMsgV0 protocol, it defaults to obtaining them from the unified metadata.|
|[INLONG-11907](https://github.com/apache/inlong/issues/11907)|[Feature][SDK] Transform support IN operator|
|[INLONG-11905](https://github.com/apache/inlong/issues/11905)|[Feature][SDK] TransformSDK calls the org.reflections library APIs according to version 0.9.12, not version 0.10.2.|
|[INLONG-11887](https://github.com/apache/inlong/issues/11887)|[Improve][SDK] Enhance the .so file copy logic of the Python SDK installation script|
|[INLONG-11873](https://github.com/apache/inlong/issues/11873)|[Feature][SDK] Support parsing field values from extended parameters|
|[INLONG-11871](https://github.com/apache/inlong/issues/11871)|[Bug] Go programs importing the DataProxy SDK may crash with no way to handle the error|
|[INLONG-11869](https://github.com/apache/inlong/issues/11869)|[Improve][SDK]Obtain a valid ip during initialization|
|[INLONG-11865](https://github.com/apache/inlong/issues/11865)|[Improve][SDK]Obtain a valid local IP address|
|[INLONG-11863](https://github.com/apache/inlong/issues/11863)|[Improve][SDK] Enhance Event Attribute Validation and Decoding Logic in SDK|
|[INLONG-11861](https://github.com/apache/inlong/issues/11861)|[Bug][CI] Compile dataproxy-sdk fail when there is no .git dictionary in the base path|

### Sort
|ISSUE|Summary|
|:--|:--|
|[INLONG-12021](https://github.com/apache/inlong/issues/12021)|[Improve][CVE] Elasticsearch Uncontrolled Resource Consumption Vulnerability|
|[INLONG-12007](https://github.com/apache/inlong/issues/12007)|[Feature][Sort] SortPulsar supports topic concatenation|
|[INLONG-11982](https://github.com/apache/inlong/issues/11982)|[Bug][Sort] Null Point Exception when changelogAuditKey is not set|
|[INLONG-11974](https://github.com/apache/inlong/issues/11974)|[Feature][Sort] Upgrade pulsar sdk version to 4.0.3|
|[INLONG-11966](https://github.com/apache/inlong/issues/11966)|[Feature][Sort] The deserialization process supports returning the data byte size in one rowdata|
|[INLONG-11958](https://github.com/apache/inlong/issues/11958)|[Feature][Sort] Allow SortCkafka to filter out data in TransformFunction|
|[INLONG-11944](https://github.com/apache/inlong/issues/11944)|[Feature][Sort] TransformFunction: parse_url supports parsing URL query strings|
|[INLONG-11942](https://github.com/apache/inlong/issues/11942)|[Feature][Sort] TransformFunction: url_decode supports specifying character sets|
|[INLONG-11943](https://github.com/apache/inlong/issues/11943)|[Feature][Sort] TransformFunction: url_encode supports specifying character sets|
|[INLONG-11939](https://github.com/apache/inlong/issues/11939)|[Feature][Sort] Support that TaskConfig merge issue preventing configuration changes from taking effect|
|[INLONG-11937](https://github.com/apache/inlong/issues/11937)|[Feature][Sort] Allow SortHttp to filter out data in TransformFunction|
|[INLONG-11931](https://github.com/apache/inlong/issues/11931)|[Feature][Sort] Optimize Transform's CSV/KV parsing|
|[INLONG-11926](https://github.com/apache/inlong/issues/11926)|[Feature][SDK] If SortSDK fails to retrieve GroupId and StreamId from the InLongMsgV0 protocol, it defaults to obtaining them from the unified metadata.|
|[INLONG-11911](https://github.com/apache/inlong/issues/11911)|[Feature][Sort] SortStandalone supports routing PB-format data streams to Kafka and Pulsar|
|[INLONG-11909](https://github.com/apache/inlong/issues/11909)|[Feature][Sort] Optimize the generation of tube session key|
|[INLONG-11902](https://github.com/apache/inlong/issues/11902)|[Feature][Sort] Allow SortCls to filter out data in TransformFunction|
|[INLONG-11892](https://github.com/apache/inlong/issues/11892)|[Feature][Sort] SortStandalone support Negative Acknowledgment mechanism for delivery failures|
|[INLONG-11888](https://github.com/apache/inlong/issues/11888)|[Feature][Sort] Sort ElasticSearch supports format conversion and data filtering via Key/Value data format|
|[INLONG-11885](https://github.com/apache/inlong/issues/11885)|[Feature][Sort] Unified Metadata supports delayed and phased decommissioning of metadata configurations.|
|[INLONG-11883](https://github.com/apache/inlong/issues/11883)|[Feature][Sort] Sort CLS supports format conversion and data filtering via Transform Functions.|

### Audit
|ISSUE|Summary|
|:--|:--|
|[INLONG-11999](https://github.com/apache/inlong/issues/11999)|[Feature][Audit] Add alert evaluation and periodic audit check task|
|[INLONG-11998](https://github.com/apache/inlong/issues/11998)|[Feature][Audit] Implement APIs for Audit Tool to interact with Manager|
|[INLONG-12000](https://github.com/apache/inlong/issues/12000)|[Feature][Audit] Implement API for obtaining audit data in Audit Tool|
|[INLONG-11998](https://github.com/apache/inlong/issues/11998)|[Feature][Audit] Implement APIs for Audit Tool to interact with Manager|
|[INLONG-11993](https://github.com/apache/inlong/issues/11993)|[Improve][Audit] The audit service supports customizing the cache on and off|
|[INLONG-11900](https://github.com/apache/inlong/issues/11900)|[Improve][Audit] In the case of no data, the Audit OpenAPI defaults to 0|

### TubeMQ
|ISSUE|Summary|
|:--|:--|
|[INLONG-11948](https://github.com/apache/inlong/issues/11948)|[Bug][Docker] TubeMQ image build failed|
