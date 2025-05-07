
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

# Release InLong 2.2.0 - Released (as of 2025-05-07)
### Agent
|ISSUE|Summary|
|:--|:--|
|[INLONG-11815](https://github.com/apache/inlong/issues/11815)|[Improve][Agent] Add a unified reporting point for events|
|[INLONG-11813](https://github.com/apache/inlong/issues/11813)|[Improve][Agent] Add Dataproxy SDK debug log|
|[INLONG-11811](https://github.com/apache/inlong/issues/11811)|[Improve][Agent] Increase the retention time of offset, default to 7 days|
|[INLONG-11778](https://github.com/apache/inlong/issues/11778)|[Improve][Agent] Separate the logs of the DataProxy SDK|
|[INLONG-11776](https://github.com/apache/inlong/issues/11776)|[Improve][Agent] Optimize the mechanism for controlling the number of instances|
|[INLONG-11774](https://github.com/apache/inlong/issues/11774)|[Improve][Agent] Modify the lifecycle of the DataProxy SDK object|
|[INLONG-11762](https://github.com/apache/inlong/issues/11762)|[Improve][Agent] Modify the logic for determining the end of the data source|
|[INLONG-11760](https://github.com/apache/inlong/issues/11760)|[Improve][Agent] Increase the number of global instances control|
|[INLONG-11752](https://github.com/apache/inlong/issues/11752)|[Improve][Agent] Modify the default collection range of data|
|[INLONG-11687](https://github.com/apache/inlong/issues/11687)|[Improve][Agent] Optimize task main thread exception handling to prevent exception exits|
|[INLONG-11685](https://github.com/apache/inlong/issues/11685)|[Improve][Agent] Agent needs to modify the logic of confirming its own IP address|
|[INLONG-11681](https://github.com/apache/inlong/issues/11681)|[Bug][Agent] Duplicate file collection|

### Dashboard
|ISSUE|Summary|
|:--|:--|
|[INLONG-11799](https://github.com/apache/inlong/issues/11799)|[Bug][Dashboard] The page will not refresh after the access groupId is switched|
|[INLONG-11772](https://github.com/apache/inlong/issues/11772)|[Improve][Dashboard] User login verify fail|

### Manager
|ISSUE|Summary|
|:--|:--|
|[INLONG-11794](https://github.com/apache/inlong/issues/11794)|[Improve][Manager][Sort]Pulsar source supports setting scan.startup.mode to null|
|[INLONG-11764](https://github.com/apache/inlong/issues/11764)|[Improve][Manager] Support SQL stream source|
|[INLONG-11751](https://github.com/apache/inlong/issues/11751)|[Bug][Manager] Failed to delete inlong group for groupId|
|[INLONG-11746](https://github.com/apache/inlong/issues/11746)|[Improve][Manager] When special characters appear in the JDBC URL, sensitive parameter validation can be bypassed|
|[INLONG-11731](https://github.com/apache/inlong/issues/11731)|[Improve][Manager]Sensitive parameters are bypassed during JDBC verification processing|
|[INLONG-11722](https://github.com/apache/inlong/issues/11722)|[Improve][Manager] Pulsar source supports Inlong properties field|

### SDK
|ISSUE|Summary|
|:--|:--|
|[INLONG-11843](https://github.com/apache/inlong/issues/11843)|[Improve][SDK] Validate the input message of Golang SDK|
|[INLONG-11830](https://github.com/apache/inlong/issues/11830)|[Bug][SDK] NullPointerException while not configure factory permits|
|[INLONG-11809](https://github.com/apache/inlong/issues/11809)|[Improve][SDK] Adjust the default value of separateEventByLF to false|
|[INLONG-11788](https://github.com/apache/inlong/issues/11788)|[Improve][SDK] Add processing logic when DataProxyNodeResponse.nodeList field is null|
|[INLONG-11787](https://github.com/apache/inlong/issues/11787)|[Bug][SDK] Dataproxy Python SDK lacks mutex header file|
|[INLONG-11782](https://github.com/apache/inlong/issues/11782)|[Improve][SDK]Adjust the Sender initialization codes in example|
|[INLONG-11770](https://github.com/apache/inlong/issues/11770)|[Improve][Sort][SDK] Fix potential Null Pointer Exception|
|[INLONG-11756](https://github.com/apache/inlong/issues/11756)|[Improve][SDK] Add NPE check in MsgSender Factory class|
|[INLONG-11754](https://github.com/apache/inlong/issues/11754)|[Improve][SDK] Add the total number of in-flight requests and total size limits|
|[INLONG-11749](https://github.com/apache/inlong/issues/11749)|[Improve][SDK] Clean up unused configuration items and functions|
|[INLONG-11745](https://github.com/apache/inlong/issues/11745)|[Improve][SDK] Clean up HttpProxySender and related implementations|
|[INLONG-11743](https://github.com/apache/inlong/issues/11743)|[Improve][SDK] Adjustment of metric statistics|
|[INLONG-11741](https://github.com/apache/inlong/issues/11741)|[Improve][SDK] Clean up the MessageSender interface and related implementation classes|
|[INLONG-11738](https://github.com/apache/inlong/issues/11738)|[Improve][SDK] Optimize the metric output when MetricDataHolder exits|
|[INLONG-11734](https://github.com/apache/inlong/issues/11734)|[Improve][SDK] Optimize SDK stop processing flow|
|[INLONG-11729](https://github.com/apache/inlong/issues/11729)|[Improve][SDK] Optimize TcpClientExample and HttpClientExample codes|
|[INLONG-11727](https://github.com/apache/inlong/issues/11727)|[Improve][SDK] Replace the Sender used in the agent-plugins module with TcpMsgSender|
|[INLONG-11725](https://github.com/apache/inlong/issues/11725)|[Improve][SDK] Replace the Sender used in the agent-core module with TcpMsgSender|
|[INLONG-11720](https://github.com/apache/inlong/issues/11720)|[Improve][SDK] Optimize MsgSenderSingleFactory implementation|
|[INLONG-11719](https://github.com/apache/inlong/issues/11719)|[Improve][SDK] Replace the Sender object in the InlongSdkDirtySender class with TcpMsgSender|
|[INLONG-11717](https://github.com/apache/inlong/issues/11717)|[Improve][SDK] Add out-of-bounds check when  in getClientByRoundRobin()|
|[INLONG-11715](https://github.com/apache/inlong/issues/11715)|[Improve][SDK] Optimize metric report content|
|[INLONG-11713](https://github.com/apache/inlong/issues/11713)|[Improve][SDK] Optimize BaseMsgSenderFactory and TimeCostInfo implementation|
|[INLONG-11711](https://github.com/apache/inlong/issues/11711)|[Improve][SDK] SortSDK shares the same PulsarClient among different SortTasks to avoid performance bottlenecks caused by too many PulsarClients.|
|[INLONG-11706](https://github.com/apache/inlong/issues/11706)|[Improve][SDK] Optimize HTTP Sender implementation|
|[INLONG-11702](https://github.com/apache/inlong/issues/11702)|[Improve][SDK] Optimize Sender factory implementation|
|[INLONG-11700](https://github.com/apache/inlong/issues/11700)|[Improve][SDK] Optimize TCP message reporting Sender implementation|
|[INLONG-11698](https://github.com/apache/inlong/issues/11698)|[Improve][SDK] Optimize TCP encode and decode implementation|
|[INLONG-11695](https://github.com/apache/inlong/issues/11695)|[Improve][SDK] MessageSender related interfaces abstraction|
|[INLONG-11692](https://github.com/apache/inlong/issues/11692)|[Improve][SDK] The metadata update function abstracted to ConfigHolder|
|[INLONG-11689](https://github.com/apache/inlong/issues/11689)|[Improve][SDK] Optimize user reporting information management|
|[INLONG-11683](https://github.com/apache/inlong/issues/11683)|[Improve][SDK] Optimize the functions return of the ProxyConfigManager class|
|[INLONG-11680](https://github.com/apache/inlong/issues/11680)|[Improve][SDK] Optimize metric-related implementation|
|[INLONG-11678](https://github.com/apache/inlong/issues/11678)|[Improve][SDK] Optimize the ProxyClientConfig class|
|[INLONG-11675](https://github.com/apache/inlong/issues/11675)|[Improve][SDK] Optimize IpUtils class related implementation|
|[INLONG-11672](https://github.com/apache/inlong/issues/11672)|[Improve][SDK] Remove the implementation of org.apache.inlong.sdk.dataproxy.pb.*|
|[INLONG-11670](https://github.com/apache/inlong/issues/11670)|[Improve][SDK] Rename the ProxysdkException class name to ProxySdkException|
|[INLONG-11668](https://github.com/apache/inlong/issues/11668)|[Feature][SDK] Add max life time support for the connections in conn pool of Golang SDK|
|[INLONG-11663](https://github.com/apache/inlong/issues/11663)|[Improve][SDK]Increase the conn pool size for Golang SDK|
|[INLONG-11662](https://github.com/apache/inlong/issues/11662)|[Improve][SDK]Enable TCP keep alive for Golang SDK|
|[INLONG-11661](https://github.com/apache/inlong/issues/11661)|[Improve][SDK]Do not mark endpoint unavailable when it is the only one in Golang SDK|
|[INLONG-11660](https://github.com/apache/inlong/issues/11660)|[Improve][SDK]Close exist conns if initConns() failed in Golang SDK|
|[INLONG-11564](https://github.com/apache/inlong/issues/11564)|[Improve][SDK] DataProxy SDK Implementation Optimization|
|[INLONG-11228](https://github.com/apache/inlong/issues/11228)|[Bug][SDK]  Limitation of Transform Sql Statement Parser|
|[INLONG-10465](https://github.com/apache/inlong/issues/10465)|[Improve][SDK] Go SDK pressure test and optimization|

### Sort
|ISSUE|Summary|
|:--|:--|
|[INLONG-11841](https://github.com/apache/inlong/issues/11841)|[Improve][Sort] When the SortTask is closed, the BufferQueueChannel must be released along with synchronously releasing the GlobalBufferQueue's token|
|[INLONG-11836](https://github.com/apache/inlong/issues/11836)|[Feature][Sort] Provide SortStandalone flow control to prevent single-task blocking from affecting the normal sorting of other tasks.|
|[INLONG-11833](https://github.com/apache/inlong/issues/11833)|[Improve][Sort] Appendmode configuration ignores case|
|[INLONG-11829](https://github.com/apache/inlong/issues/11829)|[Feature][Sort] Optimize MySQL-CDC changelog audit report|
|[INLONG-11821](https://github.com/apache/inlong/issues/11821)|[Feature][Sort] kv and csv deserialization configuration supports whether to remove and automatically add escape configuration|
|[INLONG-11819](https://github.com/apache/inlong/issues/11819)|[Feature][Sort] Sort kv/csv format must support keep escape, using line delimiter and call back when parse field has exception.|
|[INLONG-11807](https://github.com/apache/inlong/issues/11807)|[Feature][Sort] Support exactly metric report in mysql-cdc case|
|[INLONG-11805](https://github.com/apache/inlong/issues/11805)|[Improve][Sort] Restored Checkpoint Id as part of Tube Connector Session Key|
|[INLONG-11803](https://github.com/apache/inlong/issues/11803)|[Bug][Sort] When there is an issue with the format of the InLongmsg body, the parsing process will throw a Null Pointer Exception (NPE).|
|[INLONG-11674](https://github.com/apache/inlong/issues/11674)|[Feature][Sort] Pulsar Source supports InlongMsg metadata|
|[INLONG-11658](https://github.com/apache/inlong/issues/11658)|[Bug][Sort] Fix the NPE of the Kafka sink error log for some exceptions without metadata information|
|[INLONG-11543](https://github.com/apache/inlong/issues/11543)|[Improve][Sort] Upgrade pulsar connector to v4.1.0|
|[INLONG-10875](https://github.com/apache/inlong/issues/10875)|[Feature][Sort]Apache Inlong official demo documentation issue|
|[INLONG-10466](https://github.com/apache/inlong/issues/10466)|[Feature][Sort] Add Iceberg connector on Flink 1.18|

### Audit
|ISSUE|Summary|
|:--|:--|
|[INLONG-11831](https://github.com/apache/inlong/issues/11831)|[Improve][Audit] Optimize the packaging of the Audit SDK|
|[INLONG-11826](https://github.com/apache/inlong/issues/11826)|[Feature][Audit] Add a new Audit item for MQ Pulsar|
|[INLONG-11823](https://github.com/apache/inlong/issues/11823)|[Improve][Audit] Add CDC audit ID for MySQL Binlog|
|[INLONG-11817](https://github.com/apache/inlong/issues/11817)|[Feature][Audit] Audit SDK supports CDC scenario audit reconciliation|

### TubeMQ
|ISSUE|Summary|
|:--|:--|
|[INLONG-11386](https://github.com/apache/inlong/issues/11386)|[Improve][TubeMQ] Use local files to save consumer group offset information |
|[INLONG-11446](https://github.com/apache/inlong/issues/11446)|[Improve][TubeMQ] Remove legacy codes |
|[INLONG-11583](https://github.com/apache/inlong/issues/11583)|[Bug][TubeMQ] Go SDK load balance logic not perfect, causing consumption to stop suddenly |
|[INLONG-11587](https://github.com/apache/inlong/issues/11587)|[Bug][TubeMQ] Go SDK change filter order cause inconsistency error when registering to master. |
