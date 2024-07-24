
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

# Release InLong 1.13.0 - Released (as of 2024-07-18)
### Agent
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>                                                                                                                                               |
|:--------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-10094](https://github.com/apache/inlong/issues/10094) |[Improve][Agent] The task record for data supplementation has not expired|
| [INLONG-10107](https://github.com/apache/inlong/issues/10107) |[Improve][Agent] There is a bug in updating the module configuration|
| [INLONG-10113](https://github.com/apache/inlong/issues/10113) |[Improve][Agent] Delete useless code|
| [INLONG-10115](https://github.com/apache/inlong/issues/10115) |[Improve][Agent] Offset needs to be changed to save in string format because some data sources have non integer positions|
| [INLONG-10187](https://github.com/apache/inlong/issues/10187) |[Improve][Agent] Need to handle situations where timeoffset is empty, default to no offset|
| [INLONG-10189](https://github.com/apache/inlong/issues/10189) |[Improve][Agent] There is a risk of agent getting stuck after SDK initialization exception|
| [INLONG-10191](https://github.com/apache/inlong/issues/10191) |[Improve][Agent] Delete useless code|
| [INLONG-10210](https://github.com/apache/inlong/issues/10210) |[Improve][Agent] The installer needs to add a script for environment initialization|
| [INLONG-10268](https://github.com/apache/inlong/issues/10268) |[Improve][Agent] Suggest not using task ID as the data version|
| [INLONG-10281](https://github.com/apache/inlong/issues/10281) |[Improve][Agent] Real time collection of files with audit data errors|
| [INLONG-10298](https://github.com/apache/inlong/issues/10298) |[Improve][Agent] The command related code is no longer useful, it is recommended to delete it|
| [INLONG-10302](https://github.com/apache/inlong/issues/10302) |[Improve][Agent] The Task base class needs to add an interface that limits the number of instances obtained|
| [INLONG-10318](https://github.com/apache/inlong/issues/10318) |[Feature][Agent] Add PostgreSQL data source for Agent|
| [INLONG-10319](https://github.com/apache/inlong/issues/10319) |[Improve][Agent] Agent should get audit id from audit sdk|
| [INLONG-10384](https://github.com/apache/inlong/issues/10384) |[Improve][Agent] Add functions to the Store interface to extend new storage plugins|
| [INLONG-10399](https://github.com/apache/inlong/issues/10399) |[Improve][Agent] Add global configurations updater|
| [INLONG-10410](https://github.com/apache/inlong/issues/10410) |[Improve][Agent] Add ZK plugin to save offset info|
| [INLONG-10443](https://github.com/apache/inlong/issues/10443) |[Improve][Agent] Put Rocksdb into the plugins module|
| [INLONG-10446](https://github.com/apache/inlong/issues/10446) |[Improve][Agent] Adjusting audit SDK address settings|
| [INLONG-10476](https://github.com/apache/inlong/issues/10476) |[Improve][Agent] The audit address should only be obtained from the manager|
| [INLONG-10535](https://github.com/apache/inlong/issues/10535) |[Improve][Agent] Support minute level tasks|
| [INLONG-10542](https://github.com/apache/inlong/issues/10542) |[Improve][Agent] Remove the deleted watch directions|
| [INLONG-10547](https://github.com/apache/inlong/issues/10547) |[Improve][Agent] Provide a clear prompt after the mq type is incorrect|
| [INLONG-10564](https://github.com/apache/inlong/issues/10564) |[Improve][Agent] Request configuration with md5 included|
| [INLONG-10598](https://github.com/apache/inlong/issues/10598) |[Improve][Agent] Delete excess unit tests|
| [INLONG-10611](https://github.com/apache/inlong/issues/10611) |[Improve][Agent] Update configuration only when the version number is increased|
| [INLONG-10633](https://github.com/apache/inlong/issues/10633) |[Improve][Agent] The initialization function of AuditUtils needs to pass in the configuration|
| [INLONG-10645](https://github.com/apache/inlong/issues/10645) |[Improve][Agent] Installer needs to add process protection|
| [INLONG-10650](https://github.com/apache/inlong/issues/10650) |[Improve][Agent] When the installer updates the configuration, it is necessary to first determine the version|

### Dashboard
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>                                               |
|:--------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-10179](https://github.com/apache/inlong/issues/10179) |[Bug][Dashboard] There are redundant All types in cluster management|
| [INLONG-10226](https://github.com/apache/inlong/issues/10226) |[Bug][Dashboard] Audit items cannot be searched|
| [INLONG-10256](https://github.com/apache/inlong/issues/10256) |[Improve][DashBoard]  Modify the data source IP item of the file data source form in the data access module|
| [INLONG-10314](https://github.com/apache/inlong/issues/10314) |[Improve][DashBoard]  Add an operation time to the operation log table|
| [INLONG-10347](https://github.com/apache/inlong/issues/10347) |[Improve][Dashboard] Add Agent IP field to PostgreSQL data source|
| [INLONG-10350](https://github.com/apache/inlong/issues/10350) |[Bug][Dashboard] Audit item search failed|
| [INLONG-10377](https://github.com/apache/inlong/issues/10377) |[Improve][DashBoard] add Source Data Field Template|
| [INLONG-10394](https://github.com/apache/inlong/issues/10394) |[Improve][DashBoard] Agent page modifies the version and sets the default values for the commands|
| [INLONG-10409](https://github.com/apache/inlong/issues/10409) |[Feature][Dashboard] Support installing agents by SSH key-based authentication|
| [INLONG-10417](https://github.com/apache/inlong/issues/10417) |[Bug][Dashboard] There is an issue with the MODE display on the approval screen, and data synchronization is not displayed|
| [INLONG-10419](https://github.com/apache/inlong/issues/10419) |[Feature][Dashboard] Automatically switch tenants when opening a page with groupId|
| [INLONG-10449](https://github.com/apache/inlong/issues/10449) |[Bug][Dashboard] The field template for selecting a new data flow does not take effect|
| [INLONG-10468](https://github.com/apache/inlong/issues/10468) |[Feature][Dashboard] Audit data showing totals and variances|
| [INLONG-10500](https://github.com/apache/inlong/issues/10500) |[Improve][Dashboard] When you edit tenants in a template, you need to change the scope to hide them|
| [INLONG-10501](https://github.com/apache/inlong/issues/10501) |[Improve][Dashboard] Modify component type to be more intuitive|
| [INLONG-10504](https://github.com/apache/inlong/issues/10504) |[Improve][Dashboard] Added details button to tag management|
| [INLONG-10584](https://github.com/apache/inlong/issues/10584) |[Improve][Dashboard] New cluster type adds sortkafka types|
| [INLONG-10592](https://github.com/apache/inlong/issues/10592) |[Improve][Dashboard] When there are too many selections in the drop-down box, omit some of them|
| [INLONG-10614](https://github.com/apache/inlong/issues/10614) |[Improve][Dashboard] The template list does not need to display Id|
| [INLONG-10617](https://github.com/apache/inlong/issues/10617) |[Improve][Dashboard] The mq type should not exist on the data synchronization page|
| [INLONG-10620](https://github.com/apache/inlong/issues/10620) |[Bug][Dashboard] Page error occurs wrong|
| [INLONG-10640](https://github.com/apache/inlong/issues/10640) |[Improve][Dashboard]  Approval page display item modification|
| [INLONG-10651](https://github.com/apache/inlong/issues/10651) |[Improve][Dashboard] File Type data stream supports minute-level periods|
| [INLONG-10681](https://github.com/apache/inlong/issues/10681) |[Bug][Dashboard] There is a nesting problem in moduleIdList|
| [INLONG-10691](https://github.com/apache/inlong/issues/10691) |[Improve][Dashboard] Added monitoring and auditing page|

### Tube
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>                                               |
|:--------------------------------------------------------------|:-------------------------------------------------------------------------------------|
| [INLONG-10126](https://github.com/apache/inlong/issues/10126) |[Bug][tubemq-server] Maven sleepycat je.version 7.3.7 can not found in maven repository|

### DataProxy
|             <div style="width:150px">ISSUE</div>              | <div style="width:950px">Summary</div>                                                             |
|:-------------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-10066](https://github.com/apache/inlong/issues/10066) |[Improve][DataProxy] Optimize the related configuration settings in the CommonConfigHolder.java file|
| [INLONG-10067](https://github.com/apache/inlong/issues/10067) |[Improve][DataProxy] Simplify the configuration and acquisition of the Manager address|                                                                     |
| [INLONG-10080](https://github.com/apache/inlong/issues/10080) |[Improve][DataProxy] DataProxy implementation optimization|
| [INLONG-10081](https://github.com/apache/inlong/issues/10081) |[Improve][DataProxy] Modify the data format of metadata saved in the metadata.json file|
| [INLONG-10102](https://github.com/apache/inlong/issues/10102) |[Improve][DataProxy] Adjust the position where Source calls addSourceReportInfo()|
| [INLONG-10111](https://github.com/apache/inlong/issues/10111) |[Improve][DataProxy] Add auditVersion field processing|
| [INLONG-10313](https://github.com/apache/inlong/issues/10313) |[Improve][DataProxy] Replace audit ID macro with audit API|
| [INLONG-10441](https://github.com/apache/inlong/issues/10441) |[Improve][DataProxy] DataProxy supports obtaining Audit-Proxy through InLong Manager|


### Manager
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>                                                            |
|:--------------------------------------------------------------|:--------------------------------------------------------------------------------------------------|
| [INLONG-10056](https://github.com/apache/inlong/issues/10056) |[Feature][Manager] Support new manager plugin for flink 1.18|
| [INLONG-10071](https://github.com/apache/inlong/issues/10071) |[Improve][Manager] Supplementary statement trailing semicolon in apache_inlong_manager.sql|
| [INLONG-10074](https://github.com/apache/inlong/issues/10074) |[Bug][Manager] Error in querying audit information based on IP address|
| [INLONG-10076](https://github.com/apache/inlong/issues/10076) |[Bug][Manager] Data type 'doris' not support|
| [INLONG-10096](https://github.com/apache/inlong/issues/10096) |[Improve][Manager] Support installing agents by SSH|
| [INLONG-10105](https://github.com/apache/inlong/issues/10105) |[Bug][Manager]Fix sort standalone get kafka config error|
| [INLONG-10135](https://github.com/apache/inlong/issues/10135) |[Improve][Manager] Move inlongCompressType to clustertag configuration|
| [INLONG-10136](https://github.com/apache/inlong/issues/10136) |[Improve][Manager] Clean up ElasticSearch and ClickHouse audit source query function|
| [INLONG-10141](https://github.com/apache/inlong/issues/10141) |[Improve][Manager] Data preview supports returning header and specific field information for inlong msg v1|
| [INLONG-10150](https://github.com/apache/inlong/issues/10150) |[Improve][Manager] Remove metrics. audit. proxy. hosts from Flink sort plugins. properties|
| [INLONG-10169](https://github.com/apache/inlong/issues/10169) |[Improve][Manager]Support configure sort cluster for kafka|
| [INLONG-10176](https://github.com/apache/inlong/issues/10176) |[Bug][Manager] Table creation statement error|
| [INLONG-10181](https://github.com/apache/inlong/issues/10181) |[Improve][Manager] Remove useless exceptions for DataNodeOperator.getNodeConfig|
| [INLONG-10197](https://github.com/apache/inlong/issues/10197) |[Improve][Manager] Support OpenAPI for querying audit data|
| [INLONG-10200](https://github.com/apache/inlong/issues/10200) |[Improve][Manager] Define module type mapping relationships|
| [INLONG-10204](https://github.com/apache/inlong/issues/10204) |[Feature][Manager]  Kafka sink supports automatic allocation of sort standalone cluster|
| [INLONG-10233](https://github.com/apache/inlong/issues/10233) |[Improve][Manager] Report heartbeat removal port restriction|
| [INLONG-10245](https://github.com/apache/inlong/issues/10245) |[Improve][Manager] Support setting audit version for file collection|
| [INLONG-10247](https://github.com/apache/inlong/issues/10247) |[Feature][Manager] Support schedule information management for offline sync|
| [INLONG-10249](https://github.com/apache/inlong/issues/10249) |[Bug][Manager]Duplicate data appears during data preview|
| [INLONG-10252](https://github.com/apache/inlong/issues/10252) |[Bug][Manager] The audit ip which taskmanager received was wrong|
| [INLONG-10260](https://github.com/apache/inlong/issues/10260) |[Bug][Manager] Correct the wrong create table statement|
| [INLONG-10261](https://github.com/apache/inlong/issues/10261) |[Feature][Manager] Support installing agents by SSH key-based authentication|
| [INLONG-10265](https://github.com/apache/inlong/issues/10265) |[Bug][Manager] Correct wrong starrocks create table statement|
| [INLONG-10266](https://github.com/apache/inlong/issues/10266) |[Bug][Manager] Password is overwritten when adding field information|
| [INLONG-10270](https://github.com/apache/inlong/issues/10270) |[Improve][Manager] Data source tasks allow for multiple IPs|
| [INLONG-10277](https://github.com/apache/inlong/issues/10277) |[Improve][Manager] Support calling API to refresh cluster config|
| [INLONG-10279](https://github.com/apache/inlong/issues/10279) |[Improve][Manager] Support compress unified configuration|
| [INLONG-10283](https://github.com/apache/inlong/issues/10283) |[Bug][Manager] Migration of groups does not support multiple groups migrating to the same tenant|
| [INLONG-10284](https://github.com/apache/inlong/issues/10284) |[Improve][Manager] Change the default flink version from 1.13 to 1.15|
| [INLONG-10290](https://github.com/apache/inlong/issues/10290) |[Improve][Manager] Prohibit groups that have not been successfully configured from obtaining dataproxy addresses|
| [INLONG-10300](https://github.com/apache/inlong/issues/10300) |[Improve][Manager] Allow unsubmitted groups to modify mq type|
| [INLONG-10305](https://github.com/apache/inlong/issues/10305) |[Improve][Manager] Delete k8s related parameters in file collection|
| [INLONG-10324](https://github.com/apache/inlong/issues/10324) |[Bug][Manager] UseExtendedFields in Stream incorrectly overwritten|
| [INLONG-10326](https://github.com/apache/inlong/issues/10326) |[Improve][Manager] Support obtaining tenant information based on groupId|
| [INLONG-10328](https://github.com/apache/inlong/issues/10328) |[Improve][Manager] Support automatic synchronization of stream fields to sink|
| [INLONG-10330](https://github.com/apache/inlong/issues/10330) |[Improve][Manager] Support field template management|
| [INLONG-10335](https://github.com/apache/inlong/issues/10335) |[Improve][Manager] Use audit sdk to obtain audit ID|
| [INLONG-10345](https://github.com/apache/inlong/issues/10345) |[Bug][Manager] Error in obtaining audit id value|
| [INLONG-10351](https://github.com/apache/inlong/issues/10351) |[Bug][Manager] No auditname returned when obtaining audit information|
| [INLONG-10353](https://github.com/apache/inlong/issues/10353) |[Improve][Manager] refactor code for building and submitting flink job|
| [INLONG-10360](https://github.com/apache/inlong/issues/10360) |[Improve][Manager] Combine schedule state transition with group operations|
| [INLONG-10362](https://github.com/apache/inlong/issues/10362) |[Improve][Manager] Simplify code for InLong group management|
| [INLONG-10363](https://github.com/apache/inlong/issues/10363) |[Improve][Manager] Support template multi tenant management|
| [INLONG-10368](https://github.com/apache/inlong/issues/10368) |[Improve][Manager] Data preview supports obtaining message attribute information|
| [INLONG-10370](https://github.com/apache/inlong/issues/10370) |[Improve][Manager] Support configuration of kV data format|
| [INLONG-10373](https://github.com/apache/inlong/issues/10373) |[Improve][Manager] Manager client support template operation|
| [INLONG-10375](https://github.com/apache/inlong/issues/10375) |[Improve][Manager] Add field mapping strategy for CLS, StarRocks and Elasticsearch|
| [INLONG-10382](https://github.com/apache/inlong/issues/10382) |[Improve][Manager] Support obtaining node information through unique keys|
| [INLONG-10388](https://github.com/apache/inlong/issues/10388) |[Improve][Manager] Provide an interface to obtain the audit proxy address|
| [INLONG-10391](https://github.com/apache/inlong/issues/10391) |[Improve][Manager] Supports configuring zk clusters and issuing zk addresses to agents|
| [INLONG-10395](https://github.com/apache/inlong/issues/10395) |[Feature][Manager] Add interface for schedule client and engine|
| [INLONG-10396](https://github.com/apache/inlong/issues/10396) |[Feature][Manager] Support build-in schedule base on quartz|
| [INLONG-10405](https://github.com/apache/inlong/issues/10405) |[Bug][Manager] Iceberg field type mapping error |
| [INLONG-10407](https://github.com/apache/inlong/issues/10407) |[Improve][Manager] Increase the interval for determining heartbeat timeout|
| [INLONG-10413](https://github.com/apache/inlong/issues/10413) |[Improve][Manager] Support for configuring built-in fields for mysql and kafka|
| [INLONG-10415](https://github.com/apache/inlong/issues/10415) |[Improve][Manager] Kafka source supports configuring wraptype|
| [INLONG-10423](https://github.com/apache/inlong/issues/10423) |[Improve][Manager] Modify unified configuration related classes and interfaces|
| [INLONG-10425](https://github.com/apache/inlong/issues/10425) |[Bug][Manager] The kafka sink defines duplicate attributes|
| [INLONG-10430](https://github.com/apache/inlong/issues/10430) |[Bug][Manager] Check update error of unified configuration |
| [INLONG-10432](https://github.com/apache/inlong/issues/10432) |[Improve][Manager] Delete unused method getMetaConfig|
| [INLONG-10434](https://github.com/apache/inlong/issues/10434) |[Improve][Manager] Allow creating cluster nodes without setting port|
| [INLONG-10436](https://github.com/apache/inlong/issues/10436) |[Improve][Manager] move scheudle configuration from stream to group level|
| [INLONG-10438](https://github.com/apache/inlong/issues/10438) |[Improve][Manager] GetConfig does not throw an exception when obtaining the zk address fails|
| [INLONG-10452](https://github.com/apache/inlong/issues/10452) |[Improve][Manager] Delete the method of querying audit information through MySQL|
| [INLONG-10455](https://github.com/apache/inlong/issues/10455) |[Bug][Manager] Set KafkaDataNode ack fail|
| [INLONG-10459](https://github.com/apache/inlong/issues/10459) |[Feature][Manager] Support schedule instance callback to submit Flink batch job|
| [INLONG-10472](https://github.com/apache/inlong/issues/10472) |[Improve][Manager] Use audit SDK to obtain audit proxy URL|
| [INLONG-10474](https://github.com/apache/inlong/issues/10474) |[Feature][Manager] Restrict sortTaskName and sortConsumeName the same with datanodeName when migrate Inlong Group|
| [INLONG-10484](https://github.com/apache/inlong/issues/10484) | [Improve][Manager] Refactor code in manager-service module                                                      |
| [INLONG-10487](https://github.com/apache/inlong/issues/10487) | [Bug][Manager] Not return success ListenerResult                                                                |
| [INLONG-10495](https://github.com/apache/inlong/issues/10495) | [Bug][Manager] ScanStartupSubStartOffset is set to null in pulsar extranode                                     |
| [INLONG-10498](https://github.com/apache/inlong/issues/10498) | [Improve][Manager] Template supports modifying associated tenants                                               |
| [INLONG-10505](https://github.com/apache/inlong/issues/10505) | [Bug][Manager] Data preview cannot recognize null values in the data                                            |
| [INLONG-10506](https://github.com/apache/inlong/issues/10506) | [Improve][Manager] When obtaining a template, return information such as creator and modifier                   |
| [INLONG-10512](https://github.com/apache/inlong/issues/10512) | [Improve][Manager] Support preview of data in kv data type                                                      |
| [INLONG-10514](https://github.com/apache/inlong/issues/10514) | [Feature][Manager] Support built-in schedule engine trigger submitting of Flink batch job                       |
| [INLONG-10516](https://github.com/apache/inlong/issues/10516) | [Improve][Manager] Add jdbc connectors on flink 1.15 module to manager image                                    |
| [INLONG-10520](https://github.com/apache/inlong/issues/10520) | [Bug][Manager] Protocol Type reported by dataproxy not correctly accepted                                       |
| [INLONG-10543](https://github.com/apache/inlong/issues/10543) | [Improve][Manager] The delimiter and other configurations in CLS and ES sink are obtained from the stream       |
| [INLONG-10558](https://github.com/apache/inlong/issues/10558) | [Improve][Manager] Support determining whether to issue agent tasks based on the MD5 value                      |
| [INLONG-10561](https://github.com/apache/inlong/issues/10561) | [Feature][Manager] Support configrations for bounded source                                                     |
| [INLONG-10562](https://github.com/apache/inlong/issues/10562) | [Feature][Manager] SortConfig supports set start and stop consume time                                          |
| [INLONG-10589](https://github.com/apache/inlong/issues/10589) | [Improve][Manager] Rename OfflineJobSubmitRequest and remove log in controller                                  |
| [INLONG-10601](https://github.com/apache/inlong/issues/10601) | [Improve][Manager] Optimize the agent task configuration process                                                |
| [INLONG-10625](https://github.com/apache/inlong/issues/10625) | [Bug][Manager] The responsible person cannot modify the template configuration                                  |
| [INLONG-10635](https://github.com/apache/inlong/issues/10635) | [Improve][Manager] Optimize the installer configuration process                                                 |
| [INLONG-10638](https://github.com/apache/inlong/issues/10638) | [Improve][Manager] Data preview supports filtering function                                                     |
| [INLONG-10648](https://github.com/apache/inlong/issues/10648) | [Bug][Manager] The get sort config interface returns an error status code                                       |
| [INLONG-10689](https://github.com/apache/inlong/issues/10689) | [Improve][Manager] Support querying metric information                                                          |

### SDK
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>          |
|:--------------------------------------------------------------|:------------------------------------------------|
| [INLONG-10109](https://github.com/apache/inlong/issues/10109) |[Feature][SDK] Support to transform from Json protocol to CSV/KV protocol by single SQL|
| [INLONG-10117](https://github.com/apache/inlong/issues/10117) |[Feature][SDK] Support to transform from PB protocol to CSV/KV protocol by single SQL|
| [INLONG-10129](https://github.com/apache/inlong/issues/10129) |[Feature][SDK] Transform SQL support +-*/ operations|
| [INLONG-10130](https://github.com/apache/inlong/issues/10130) |[Feature][SDK] Transform SQL support string concat function|
| [INLONG-10154](https://github.com/apache/inlong/issues/10154) |[Feature][SDK] Support to transform CSV/KV data to CSV/KV data without field list configuration|
| [INLONG-10213](https://github.com/apache/inlong/issues/10213) |[Feature][SDK] SortSDK support unified sort configuration|
| [INLONG-10221](https://github.com/apache/inlong/issues/10221) |[Improve][SDK] DataProxy SDK of cpp supports automatic installation of log4cplus components|
| [INLONG-10291](https://github.com/apache/inlong/issues/10291) |[Bug][SDK] Incorrect initializing of gnet in Golang SDK|
| [INLONG-10292](https://github.com/apache/inlong/issues/10292) |[Bug][SDK] Panic in `connpool.UpdateEndpoints()` of Golang SDK|
| [INLONG-10427](https://github.com/apache/inlong/issues/10427) |[Feature][SDK] The Go SDK supports authentication for Manager access|
| [INLONG-10457](https://github.com/apache/inlong/issues/10457) |[Bug][SDK] Fix auth spelling errors|
| [INLONG-10522](https://github.com/apache/inlong/issues/10522) |[Feature][SDK] SortSDK support assgin subscription|
| [INLONG-10531](https://github.com/apache/inlong/issues/10531) |[Feature][SDK] Add InLong Dataproxy Python SDK based on C++ SDK|
| [INLONG-10532](https://github.com/apache/inlong/issues/10532) |[Improve][SDK] Add InLong Dataproxy Python SDK sample|
| [INLONG-10534](https://github.com/apache/inlong/issues/10534) |[Umbrella][SDK] InLong Dataproxy Python SDK |
| [INLONG-10603](https://github.com/apache/inlong/issues/10603) |[Feature][SDK] Transform SQL support arithmetic functions(Including power, abs, sqrt and ln)|
| [INLONG-10607](https://github.com/apache/inlong/issues/10607) |[Feature][SDK] Transform SQL support arithmetic functions(Including log10, log2, log and exp)|
| [INLONG-10652](https://github.com/apache/inlong/issues/10652) |[Improve][SDK] Inlong Transform support for generics|
| [INLONG-10668](https://github.com/apache/inlong/issues/10668) |[Improve][SDK] Add rebalance and recover support of conn pool in Golang SDK|
| [INLONG-10670](https://github.com/apache/inlong/issues/10670) |[Bug][SDK] Potential block in Golang SDK|
| [INLONG-10676](https://github.com/apache/inlong/issues/10676) |[Improve][SDK] Fix type asserttion and type switch warnings in Golang SDK|
| [INLONG-10677](https://github.com/apache/inlong/issues/10677) |[Improve][SDK] Bump up modules in Golang SDK|
| [INLONG-10675](https://github.com/apache/inlong/issues/10675) |[Improve][SDK] Use exponential backoff instead of linear backoff retrying in Golang SDK|

### Sort
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>                                                                                                |
|:--------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8464](https://github.com/apache/inlong/issues/8464)   |[Feature] [Sort] Add JDBC connector on Flink 1.15|
| [INLONG-10053](https://github.com/apache/inlong/issues/10053) |[Feature][Sort] Support flink-connector-pulsar based on flink 1.18|
| [INLONG-10054](https://github.com/apache/inlong/issues/10054) |[Feature][Sort] Add dependencies for flink 1.18|
| [INLONG-10055](https://github.com/apache/inlong/issues/10055) |[Feature][Sort] Support flink-connector-jdbc based on flink 1.18|                                                              |
| [INLONG-10069](https://github.com/apache/inlong/issues/10069) |[Feature][Sort] Support audit metrics for sort-connector-pulsar-1.18|
| [INLONG-10091](https://github.com/apache/inlong/issues/10091) |[Feature][Sort] Add json format for 1.18|
| [INLONG-10144](https://github.com/apache/inlong/issues/10144) |[Feature][Sort] Redis connectors support audit ID|
| [INLONG-10152](https://github.com/apache/inlong/issues/10152) |[Improve][Sort] Refactor MetricOption code structure.|
| [INLONG-10159](https://github.com/apache/inlong/issues/10159) |[Feature][Sort] Kafka connector support audit ID|
| [INLONG-10164](https://github.com/apache/inlong/issues/10164) |[Umbrella][Sort] SortStandalone support unified SortConfig|
| [INLONG-10173](https://github.com/apache/inlong/issues/10173) |[Feature][Sort] SortStandalone support request unified configuration|
| [INLONG-10183](https://github.com/apache/inlong/issues/10183) |[Feature][Sort] MongoDB connector support audit ID|
| [INLONG-10193](https://github.com/apache/inlong/issues/10193) |[Feature][Sort] Postgres connector support audit ID|
| [INLONG-10194](https://github.com/apache/inlong/issues/10194) |[Feature][Sort] Sqlserver connector support audit ID|
| [INLONG-10208](https://github.com/apache/inlong/issues/10208) |[Feature][Sort] ClsSink support unified configuration|
| [INLONG-10224](https://github.com/apache/inlong/issues/10224) |[Feature][Sort] Unified configuration check utils|
| [INLONG-10228](https://github.com/apache/inlong/issues/10228) |[Feature][Sort] PulsarSink support unified configuration|
| [INLONG-10229](https://github.com/apache/inlong/issues/10229) |[Feature][Sort] EsSink support unified configuration|
| [INLONG-10230](https://github.com/apache/inlong/issues/10230) |[Feature][Sort] KafkaSink support unified configuration|
| [INLONG-10257](https://github.com/apache/inlong/issues/10257) |[Improve][Sort] Upgrade flink version from 1.13.6 to 1.15.4|
| [INLONG-10272](https://github.com/apache/inlong/issues/10272) |[Improve][Sort] Unified configuration check utils support check latest config|
| [INLONG-10296](https://github.com/apache/inlong/issues/10296) |[Bug][Sort] Connectors AuditOperator  was not serialized|
| [INLONG-10297](https://github.com/apache/inlong/issues/10297) |[Bug][Sort] The audit operator in mysql connector cannot be serialized and the job cannot submit to jobmanager|
| [INLONG-10311](https://github.com/apache/inlong/issues/10311) |[Feature][Sort] TubeMQ source support report audit information exactly once|
| [INLONG-10312](https://github.com/apache/inlong/issues/10312) |[Feature][Sort] Iceberg sink support report audit information exactly once|
| [INLONG-10317](https://github.com/apache/inlong/issues/10317) |[Feature][Sort] Kafka Source support report audit information exactly once|
| [INLONG-10323](https://github.com/apache/inlong/issues/10323) |[Feature][Sort] Support Kv deserialization in sort module|
| [INLONG-10338](https://github.com/apache/inlong/issues/10338) |[Bug][Sort] Sqlserver connector's AuditOperator was not serialized |
| [INLONG-10339](https://github.com/apache/inlong/issues/10339) |[Bug][Sort] PostgreSQL connector's AuditOperator was not serialized|
| [INLONG-10340](https://github.com/apache/inlong/issues/10340) |[Bug][Sort] MongoDB connector's AuditOperator was not serialized|
| [INLONG-10355](https://github.com/apache/inlong/issues/10355) |[Feature][Sort] Iceberg source support report audit information exactly once|
| [INLONG-10357](https://github.com/apache/inlong/issues/10357) |[Feature][Sort] Starrocks sink support report audit information exactly once|
| [INLONG-10358](https://github.com/apache/inlong/issues/10358) |[Feature][Sort] Pulsar source support report audit information exactly once|
| [INLONG-10401](https://github.com/apache/inlong/issues/10401) |[Improve][Sort] Add metadata for Mysql connector and relocate debezium dependencies |
| [INLONG-10489](https://github.com/apache/inlong/issues/10489) |[Bug][Sort] Mongodb2StarRocksTest sometime occur error when in workflow|
| [INLONG-10492](https://github.com/apache/inlong/issues/10492) |[Bug][Sort] Init failure of pulsar connector|
| [INLONG-10508](https://github.com/apache/inlong/issues/10508) |[Bug][Sort] Fix pulsar connector flink 1.15 parameter cannot keep consistent with flink 1.13 |
| [INLONG-10526](https://github.com/apache/inlong/issues/10526) |[Improve][Sort] ClsSink support switch metadata acquire mode|
| [INLONG-10527](https://github.com/apache/inlong/issues/10527) |[Improve][Sort] EsSink support switch metadata acquire mode|
| [INLONG-10528](https://github.com/apache/inlong/issues/10528) |[Improve][Sort] KafkaSink support switch metadata acquire mode|
| [INLONG-10529](https://github.com/apache/inlong/issues/10529) |[Improve][Sort] PulsarSink support switch metadata acquire mode|
| [INLONG-10530](https://github.com/apache/inlong/issues/10530) |[Umbrella][Sort] Sortstandalone support switch metadata acquire mode|
| [INLONG-10540](https://github.com/apache/inlong/issues/10540) |[Feature][Sort] SortStandalone unified configuration metric reporter|
| [INLONG-10555](https://github.com/apache/inlong/issues/10555) |[Feature][Sort] SortStandalone support report the difference between two configuration|
| [INLONG-10560](https://github.com/apache/inlong/issues/10560) |[Feature][Sort] Support bounded pulsar source|
| [INLONG-10568](https://github.com/apache/inlong/issues/10568) |[Improve][Sort] The starrocks connector UNKNOWN datatype handle method need to change|
| [INLONG-10573](https://github.com/apache/inlong/issues/10573) |[Improve][Sort] Pulsar source connector report audit attach input time|
| [INLONG-10575](https://github.com/apache/inlong/issues/10575) |[Improve][Sort] Mysql source support report audit information exactly once|
| [INLONG-10577](https://github.com/apache/inlong/issues/10577) |[Feature][Sort] Simplified SortStandalone SortSdkSource|
| [INLONG-10594](https://github.com/apache/inlong/issues/10594) |[Improve][Sort] Provide default kafka producer configuration|
| [INLONG-10597](https://github.com/apache/inlong/issues/10597) |[Improve][Sort] Provide default pulsar producer configuration|
| [INLONG-10604](https://github.com/apache/inlong/issues/10604) |[Bug][Sort] NPE when unified configuration is not exits|
| [INLONG-10609](https://github.com/apache/inlong/issues/10609) |[Improve][Sort] PostgreSQL source support report audit information exactly once|
| [INLONG-10610](https://github.com/apache/inlong/issues/10610) |[Improve][Sort] MongoDB source support report audit information exactly once|
| [INLONG-10623](https://github.com/apache/inlong/issues/10623) |[Feature][Sort] The Pulsar connector on flink1.18 not set audit time, it should be set as input time through 'consume_time' metadata field|
| [INLONG-10628](https://github.com/apache/inlong/issues/10628) |[Feature][Sort] The end2end test env on flink1.18 has not implemented|
| [INLONG-10630](https://github.com/apache/inlong/issues/10630) |[Feature][Sort] SQL Server source support report audit information exactly once|
| [INLONG-10655](https://github.com/apache/inlong/issues/10655) |[Improve][Sort] Kafka and Pulsar Sink support parse stream separator|
| [INLONG-10682](https://github.com/apache/inlong/issues/10682) |[Improve][Sort] Pulsar source sending audit information support exactly once and helper.validateExcept should add ExtractNode.INLONG_MSG|
| [INLONG-10696](https://github.com/apache/inlong/issues/10696) |[Improve][Sort] StarRocks sink supports ignore json parse error|
| [INLONG-10694](https://github.com/apache/inlong/issues/10694) |[Bug][Sort] Mysql-cdc source fails to convert timestamp type|

### Audit
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>                                        |
|:--------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-10085](https://github.com/apache/inlong/issues/10085) |[Improve][Audit] Optimize the performance of audit-service|
| [INLONG-10089](https://github.com/apache/inlong/issues/10089) |[Improve][Audit] Adjust the service configuration of Audit|
| [INLONG-10122](https://github.com/apache/inlong/issues/10122) |[Improve][Audit] Update the apache_inlong_audit.sql to apache_inlong_audit_mysql.sql|
| [INLONG-10132](https://github.com/apache/inlong/issues/10132) |[Improve][Audit] Clean up Elasticsearch and ClickHouse related code of audit-store|
| [INLONG-10147](https://github.com/apache/inlong/issues/10147) |[Improve][Audit] Less usage costs when the Audit uses the MySQL as storage|
| [INLONG-10171](https://github.com/apache/inlong/issues/10171) |[Improve][Audit] Update the standalone deploy scripts for the audit service|
| [INLONG-10201](https://github.com/apache/inlong/issues/10201) |[Improve][Audit] Rename configuration variables|
| [INLONG-10225](https://github.com/apache/inlong/issues/10225) |[Improve][Audit] Audit SDK provides the ability to automatically assign and manage Audit ID|
| [INLONG-10242](https://github.com/apache/inlong/issues/10242) |[Improve][Audit] Audit SDK compatible with InLong Manager to manage audit items|
| [INLONG-10263](https://github.com/apache/inlong/issues/10263) |[Improve][Audit] Solve the conflict between the jdbc.url parameter of Audit Store and the container environment variable|
| [INLONG-10274](https://github.com/apache/inlong/issues/10274) |[Improve][Audit] The OpenAPI  of Audit Service returns the average transmission time|
| [INLONG-10306](https://github.com/apache/inlong/issues/10306) |[Improve][Audit] Compatible with scenarios where the Audit Tag is empty|
| [INLONG-10321](https://github.com/apache/inlong/issues/10321) |[Improve][Audit] Audit supports the Audit Proxy service discovery and management|
| [INLONG-10321](https://github.com/apache/inlong/issues/10321) |[Improve][Audit] Audit supports the Audit Proxy service discovery and management|
| [INLONG-10365](https://github.com/apache/inlong/issues/10365) |[Improve][Audit] Optimizing TCP sticky packets may lead to duplication of audit data|
| [INLONG-10379](https://github.com/apache/inlong/issues/10379) |[Improve][Audit] Add HDFS Audit items in the Audit SDK|
| [INLONG-10387](https://github.com/apache/inlong/issues/10387) |[Improve][Audit]  Audit SDK supports obtaining Audit-Proxy capabilities through InLong Manager|
| [INLONG-10402](https://github.com/apache/inlong/issues/10402) |[Improve][Audit]  Audit Service supports the hourly Audit data one day ago|
| [INLONG-10461](https://github.com/apache/inlong/issues/10461) |[Bug][Audit] Cause HttpHostConnectException for manager |
| [INLONG-10470](https://github.com/apache/inlong/issues/10470) |[Improve][Audit]  Optimize Audit Proxy configuration update retry logic|
| [INLONG-10478](https://github.com/apache/inlong/issues/10478) |[Improve][Audit] Default to enable audit for the Docker and Standalone deployment|
| [INLONG-10480](https://github.com/apache/inlong/issues/10480) |[Improve][Audit] Audit Service  automatically manages MySQL partitions|
| [INLONG-10481](https://github.com/apache/inlong/issues/10481) |[Improve][Audit] Optimize Audit domain management|
| [INLONG-10687](https://github.com/apache/inlong/issues/10687) |[Improve][Audit] Independent the Audit items of Agent from module reconciliation|


### Other
| <div style="width:150px">ISSUE</div>                          | <div style="width:950px">Summary</div>                                        |
|:---------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-10051](https://github.com/apache/inlong/issues/10051) |[Improve][Github] improve the pull request template|
| [INLONG-10057](https://github.com/apache/inlong/issues/10057) |[Feature][Distribution] Support assembling flink 1.18 dependencies and connectors|
| [INLONG-10064](https://github.com/apache/inlong/issues/10064) |[Feature][Tool] Support multi-version flinks in inlong-dev-toolkit|
| [INLONG-10120](https://github.com/apache/inlong/issues/10120) |[Improve][CI] Update the actions/upload-artifact to V4|
| [INLONG-10124](https://github.com/apache/inlong/issues/10124) |[Bug][deploy] In 1.12 the single-machine deployment fails|
| [INLONG-10125](https://github.com/apache/inlong/issues/10125) |[Bug][Script] In 1.11 single-machine deployment, the agent module cannot be started|
| [INLONG-10138](https://github.com/apache/inlong/issues/10138) |[Improve][CI] Node.js 16 actions are deprecated|
| [INLONG-10160](https://github.com/apache/inlong/issues/10160) |[Improve][CVE] Elasticsearch vulnerable to Uncontrolled Resource Consumption|
| [INLONG-10161](https://github.com/apache/inlong/issues/10161) |[Improve][CVE] Golang protojson.Unmarshal function infinite loop when unmarshaling certain forms of invalid JSON|
| [INLONG-10162](https://github.com/apache/inlong/issues/10162) |[Improve][CVE] net/http, x/net/http2: close connections when receiving too many headers|
| [INLONG-10205](https://github.com/apache/inlong/issues/10205) |[Improve][Script] Add the metrics.audit.proxy.hosts for standalone deployment|
| [INLONG-10331](https://github.com/apache/inlong/issues/10331) |[Improve][CVE] Decompressors can crash the JVM and leak memory content in Aircompressor|
| [INLONG-10582](https://github.com/apache/inlong/issues/10582) |[Improve][ASF] Disable merge and rebase merge|











