
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

# Release InLong 1.9.0 - Released (as of 2023-09-18)
### Agent
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8850](https://github.com/apache/inlong/issues/8850) | [Improve][Agent] Remove unregister of MetricRegister when taskmanager is initialized                                            |
| [INLONG-8655](https://github.com/apache/inlong/issues/8655) | [Bug][Agent] JobWrapper thread leaks when the job is stopped                                                                    |
| [INLONG-8652](https://github.com/apache/inlong/issues/8652) | [Improve][Agent] Delete the capacity of setting blacklist                                                                       |
| [INLONG-8649](https://github.com/apache/inlong/issues/8649) | [Bug][Agent] Thread leaks for ProxySink when the DataProxy SDK init failed                                                      |
| [INLONG-8647](https://github.com/apache/inlong/issues/8647) | [Improve][Agent] Stop sending task snapshot to Manager module                                                                   |
| [INLONG-8645](https://github.com/apache/inlong/issues/8645) | [Improve][Agent] Delete the capacity of the loading trigger for local files                                                     |
| [INLONG-8629](https://github.com/apache/inlong/issues/8629) | [Bug][Agent] Sending invalid data to DataProxy failed blocks normal data sending                                                |
| [INLONG-8524](https://github.com/apache/inlong/issues/8524) | [Improve][Agent] Update the JVM Options for Agent                                                                               |
| [INLONG-8520](https://github.com/apache/inlong/issues/8520) | [Bug][Agent] File agent sent data to dataproxy was all json formatted 1.9.x/bugfix                                              |
| [INLONG-8146](https://github.com/apache/inlong/issues/8146) | [Improve][Agent] Optimize agent-env.sh, and '-XX:NativeMemoryTracking'                                                          |
| [INLONG-8799](https://github.com/apache/inlong/issues/8799) | [Bug][Manager][Agent][DataProxy] The "opentelemetry" related configs may affect the startup of services                         |
| [INLONG-8611](https://github.com/apache/inlong/issues/8611) | [Feature][Manager][Agent][DataProxy] Support full link tracking to improve the observability of the project                     |
                                                                                                                                                                                                
### Audit
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8836](https://github.com/apache/inlong/issues/8836) | [Improve][Audit] Add audit_tag information to distinguish data sources and data targets                                         |
| [INLONG-8753](https://github.com/apache/inlong/issues/8753) | [Improve][Audit] Separate commons-text from org.apache.flume                                                                    |
| [INLONG-8699](https://github.com/apache/inlong/issues/8699) | [Improve][Audit] Optimize the service log of audit-proxy                                                                        |
| [INLONG-8642](https://github.com/apache/inlong/issues/8642) | [Improve][Audit] Remove the audit commons-text dependency                                                                       |

### Dashboard
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8882](https://github.com/apache/inlong/issues/8882) | [Feature][Dashboard] Support management of Pulsar data node                                                                     |
| [INLONG-8881](https://github.com/apache/inlong/issues/8881) | [Feature][Dashboard] Support management of Pulsar sink                                                                          |
| [INLONG-8843](https://github.com/apache/inlong/issues/8843) | [Improve][Dashboard] StarRocks sink field optimization                                                                          |
| [INLONG-8841](https://github.com/apache/inlong/issues/8841) | [Feature][Dashboard] Support management of Iceberg sources                                                                      |
| [INLONG-8810](https://github.com/apache/inlong/issues/8810) | [Improve][Dashboard] Approval management process ID link optimization                                                           |
| [INLONG-8800](https://github.com/apache/inlong/issues/8800) | [Improve][Dashboard] Owners search component optimization                                                                       |
| [INLONG-8788](https://github.com/apache/inlong/issues/8788) | [Improve][Dashboard] Data synchronization page optimization                                                                     |
| [INLONG-8779](https://github.com/apache/inlong/issues/8779) | [Improve][Dashboard] Modify password verification optimization                                                                  |
| [INLONG-8760](https://github.com/apache/inlong/issues/8760) | [Bug][Dashboard] The transform button was not at the center                                                                     |
| [INLONG-8757](https://github.com/apache/inlong/issues/8757) | [Improve][Dashboard] Implement buttons using a link  good first issue                                                           |
| [INLONG-8755](https://github.com/apache/inlong/issues/8755) | [Improve][Dashboard] Data synchronization field mapping optimization                                                            |
| [INLONG-8727](https://github.com/apache/inlong/issues/8727) | [Improve][Dashboard] Approval management Application details optimization                                                       |
| [INLONG-8705](https://github.com/apache/inlong/issues/8705) | [Improve][Dashboard] Source and sink title optimization                                                                         |
| [INLONG-8693](https://github.com/apache/inlong/issues/8693) | [Feature][Dashboard] Data synchronization supports Audit                                                                        |
| [INLONG-8661](https://github.com/apache/inlong/issues/8661) | [Bug][DashBoard] DatabaseWhiteList is required which is inconsistent with tips                                                  |
| [INLONG-8624](https://github.com/apache/inlong/issues/8624) | [Bug][Dashboard] Tenant Management Search Tenant Exceptions                                                                     |
| [INLONG-8621](https://github.com/apache/inlong/issues/8621) | [Feature][Dashboard] Approval management supports approval data synchronization                                                 |
| [INLONG-8575](https://github.com/apache/inlong/issues/8575) | [Improve][Dashboard] Tenant management query optimization                                                                       |
| [INLONG-8560](https://github.com/apache/inlong/issues/8560) | [Bug][Dashboard] No username and password when configuring hive.                                                                |
| [INLONG-8548](https://github.com/apache/inlong/issues/8548) | [Feature][Dashboard] Data access supports displaying transmission delay                                                         |
| [INLONG-8546](https://github.com/apache/inlong/issues/8546) | [Bug][Dashboard] Inlong group resource details display error good first issue                                                   |
| [INLONG-8512](https://github.com/apache/inlong/issues/8512) | [Bug][Dashboard] Create tube consumer failed when selecting topic 1.9.x/bugfix                                                  |
| [INLONG-8502](https://github.com/apache/inlong/issues/8502) | [Bug][Dashboard] Query data preview interface exception                                                                         |
| [INLONG-8500](https://github.com/apache/inlong/issues/8500) | [Bug][Dashboard] Fix stream data preview url error                                                                              |
| [INLONG-8008](https://github.com/apache/inlong/issues/8008) | [Improve][Dashboard] Add default icon for different data nodes good first issue                                                 |

### DataProxy
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8914](https://github.com/apache/inlong/issues/8914) | [Improve][DataProxy] Optimize DataProxy event statistics                                                                        |
| [INLONG-8899](https://github.com/apache/inlong/issues/8899) | [Improve][DataProxy] Optimize metadata update logic                                                                             |
| [INLONG-8819](https://github.com/apache/inlong/issues/8819) | [Improve][DataProxy] Optimize ConfigHolder related subclass loading processing                                                  |
| [INLONG-8758](https://github.com/apache/inlong/issues/8758) | [Improve][DataProxy] Metadata synchronization management optimization                                                           |
| [INLONG-8741](https://github.com/apache/inlong/issues/8741) | [Bug][DataProxy] Wrong constant reference in CommonConfigHolder class                                                           |
| [INLONG-8729](https://github.com/apache/inlong/issues/8729) | [Bug][DataProxy] Wrong result in the addSendResultMetric function also reports success                                          |
| [INLONG-8725](https://github.com/apache/inlong/issues/8725) | [Improve][DataProxy] Cache file metric output switch value at usage location                                                    |
| [INLONG-8679](https://github.com/apache/inlong/issues/8679) | [Improve][DataProxy] Migrate index-related variables to abstract classes                                                        |
| [INLONG-8670](https://github.com/apache/inlong/issues/8670) | [Improve][DataProxy] Define in detail the exceptions actively thrown in Source                                                  |
| [INLONG-8657](https://github.com/apache/inlong/issues/8657) | [Improve][DataProxy] Cache Source, Sink name and Channel object content                                                         |
| [INLONG-8597](https://github.com/apache/inlong/issues/8597) | [Improve][DataProxy] Adjust the format of the metric output to the file                                                         |
| [INLONG-8589](https://github.com/apache/inlong/issues/8589) | [Improve][DataProxy] Add callback parameter support for Http access                                                             |
| [INLONG-8576](https://github.com/apache/inlong/issues/8576) | [Improve][DataProxy] Adjust handling when messages are incomplete                                                               |
| [INLONG-8507](https://github.com/apache/inlong/issues/8507) | [Improve][Dataproxy] Modify nodeIp in dataproxy message to clientIp                                                             |
| [INLONG-6364](https://github.com/apache/inlong/issues/6364) | [Improve][DataProxy] Add DataProxy node load information stage/stale                                                            |
| [INLONG-8799](https://github.com/apache/inlong/issues/8799) | [Bug][Manager][Agent][DataProxy] The "opentelemetry" related configs may affect the startup of services                         |
| [INLONG-8611](https://github.com/apache/inlong/issues/8611) | [Feature][Manager][Agent][DataProxy] Support full link tracking to improve the observability of the project                     |

### Manager
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8852](https://github.com/apache/inlong/issues/8852) | [Improve][Manager] Supports fuzzy matching of username based on keyword                                                         |
| [INLONG-8845](https://github.com/apache/inlong/issues/8845) | [Feature][Manager] Support Tencent Cloud Log Service data flow                                                                  |
| [INLONG-8834](https://github.com/apache/inlong/issues/8834) | [Bug][Manager] No relationship generated after setting the transformnode                                                        |
| [INLONG-8832](https://github.com/apache/inlong/issues/8832) | [Bug][Manager] The request type for transform/list in the client does not match the actual one                                  |
| [INLONG-8829](https://github.com/apache/inlong/issues/8829) | [Improve][Manager] Support configuring whether to use zookeeper by request                                                      |
| [INLONG-8827](https://github.com/apache/inlong/issues/8827) | [Feature][Manager] Inlong manager sql directory is incorrect in docker README                                                   |
| [INLONG-8823](https://github.com/apache/inlong/issues/8823) | [Feature][Manager] Supporting data flow to Pulsar                                                                               |
| [INLONG-8816](https://github.com/apache/inlong/issues/8816) | [INLONG-8815][Manager] Supports configuring iceberg streamSources                                                               |
| [INLONG-8815](https://github.com/apache/inlong/issues/8815) | [Improve][Manager] Supports configuring iceberg streamSources                                                                   |
| [INLONG-8813](https://github.com/apache/inlong/issues/8813) | [Improve][Manager] Replacing whitespace characters in MySQL JDBC URL                                                            |
| [INLONG-8805](https://github.com/apache/inlong/issues/8805) | [Feature][Manager] Check tenant status before deleting tenant                                                                   |
| [INLONG-8799](https://github.com/apache/inlong/issues/8799) | [Bug][Manager][Agent][DataProxy] The "opentelemetry" related configs may affect the startup of services                         |
| [INLONG-8797](https://github.com/apache/inlong/issues/8797) | [Feature][Manager][Sort] Audit has no data for data sync                                                                        |
| [INLONG-8794](https://github.com/apache/inlong/issues/8794) | [Improve][Manager] Support add streamField and sinkField                                                                        |
| [INLONG-8781](https://github.com/apache/inlong/issues/8781) | [Bug][Manager] When batch task is finished, exception is throw                                                                  |
| [INLONG-8773](https://github.com/apache/inlong/issues/8773) | [Improve][Manager] Optimize Agent heartbeat logic                                                                               |
| [INLONG-8771](https://github.com/apache/inlong/issues/8771) | [Bug][Manager] Audit data error for MySQL data source                                                                           |
| [INLONG-8751](https://github.com/apache/inlong/issues/8751) | [Bug][Manager] Response of data preview was empty                                                                               |
| [INLONG-8749](https://github.com/apache/inlong/issues/8749) | [Improve][Manager] Support configuring whether to initialize node state during startup                                          |
| [INLONG-8739](https://github.com/apache/inlong/issues/8739) | [Bug][Manager] The file collection task was mistakenly deleted                                                                  |
| [INLONG-8675](https://github.com/apache/inlong/issues/8675) | [Bug][Manager]Manager client InlongTenant get method request type error                                                         |
| [INLONG-8671](https://github.com/apache/inlong/issues/8671) | [Bug][Manager] Find no method parameter of form data in POST request                                                            |
| [INLONG-8663](https://github.com/apache/inlong/issues/8663) | [Improve][Manager] Add tenant related OpenAPI                                                                                   |
| [INLONG-8627](https://github.com/apache/inlong/issues/8627) | [Improve][Manager] Add parameters validation for the testConnection method                                                      |
| [INLONG-8622](https://github.com/apache/inlong/issues/8622) | [Improve][Manager] Optimize the permission control of user API                                                                  |
| [INLONG-8620](https://github.com/apache/inlong/issues/8620) | [INLONG-8619][Manager] Remove the inlong role check of internal interfaces                                                      |
| [INLONG-8619](https://github.com/apache/inlong/issues/8619) | [Improve][Manager] Remove the inlong role check of internal interfaces                                                          |
| [INLONG-8618](https://github.com/apache/inlong/issues/8618) | [INLONG-8617][Manager] Optimize compatibility of SortSdk config interface  1.9.x/bugfix                                         |
| [INLONG-8617](https://github.com/apache/inlong/issues/8617) | [Improve][Manager] Optimize compatibility of SortSdk config interface                                                           |
| [INLONG-8611](https://github.com/apache/inlong/issues/8611) | [Feature][Manager][Agent][DataProxy] Support full link tracking to improve the observability of the project                     |
| [INLONG-8606](https://github.com/apache/inlong/issues/8606) | [INLONG-8590][Manager] Make Cluster and ClusterTags as public resources                                                         |
| [INLONG-8603](https://github.com/apache/inlong/issues/8603) | [Bug][Manager] Fix the vulnerability to security attacks for the MySQL JDBC URL                                                 |
| [INLONG-8590](https://github.com/apache/inlong/issues/8590) | [Improve][Manager] Make Cluster and ClusterTags as public resources                                                             |
| [INLONG-8586](https://github.com/apache/inlong/issues/8586) | [Improve][Manager] Stop Stream Source which is still running after group is stopped                                             |
| [INLONG-8582](https://github.com/apache/inlong/issues/8582) | [Improve][Manager] Remove unnecessary log information in InlongClusterServiceImpl                                               |
| [INLONG-8570](https://github.com/apache/inlong/issues/8570) | [Bug][Manager] Modules in the wrong order cause UT execution to fail                                                            |
| [INLONG-8568](https://github.com/apache/inlong/issues/8568) | [INLONG-8567][Manager] Add new role INLONG_SERVICE for internal service query  1.9.x/bugfix                                     |
| [INLONG-8567](https://github.com/apache/inlong/issues/8567) | [Feature][Manager] Add new role INLONG_SERVICE for internal service query                                                       |
| [INLONG-8564](https://github.com/apache/inlong/issues/8564) | [Bug][Manager] Unable to issue tasks after modifying data node info                                                             |
| [INLONG-8563](https://github.com/apache/inlong/issues/8563) | [Improve][Manager] Opitmize the permission check of tenant-related operation                                                    |
| [INLONG-8556](https://github.com/apache/inlong/issues/8556) | [Improve][Manager] Optimize the location of the manager-plugins-flink jar package                                               |
| [INLONG-8550](https://github.com/apache/inlong/issues/8550) | [INLONG-8547][Manager] Add workflow approver automatically when create a new tenant                                             |
| [INLONG-8547](https://github.com/apache/inlong/issues/8547) | [Feature][Manager] Add workflow approver automatically when create a new tenant                                                 |
| [INLONG-8541](https://github.com/apache/inlong/issues/8541) | [Bug][Manager] Save InlongGroup with error tenant 1.9.x/bugfix                                                                  |
| [INLONG-8539](https://github.com/apache/inlong/issues/8539) | [Improve][Manager] Remove stream source when heartbeat of agent contains no group message                                       |
| [INLONG-8537](https://github.com/apache/inlong/issues/8537) | [Bug][Manager] Insert group failed 1.9.x/bugfix                                                                                 |
| [INLONG-8535](https://github.com/apache/inlong/issues/8535) | [Bug][Manager] There is a null pointer when calling updateRuntimeConfig method                                                  |
| [INLONG-8529](https://github.com/apache/inlong/issues/8529) | [Improve][Manager] update stream source to heartbeat timeout when evit cluster node                                             |
| [INLONG-8522](https://github.com/apache/inlong/issues/8522) | [Improve][Manager] Optimize log print for AgentService                                                                          |
| [INLONG-8516](https://github.com/apache/inlong/issues/8516) | [Improve][Manager] Missing scala dependency for Flink 1.15 1.9.x/bugfix                                                         |
| [INLONG-8514](https://github.com/apache/inlong/issues/8514) | [Improve][Manager] Support ClickHouse field type special modifier Nullable                                                      |
| [INLONG-8509](https://github.com/apache/inlong/issues/8509) | [Improve][Manager] Optimize preProcessTemplateFileTask in AgentServiceImpl                                                      |
| [INLONG-8492](https://github.com/apache/inlong/issues/8492) | [INLONG-8490][Manager] Duplicate queried audit data according to all fields                                                     |
| [INLONG-8491](https://github.com/apache/inlong/issues/8491) | [Feature][Manager] manager client support list inlong streams with sources and sinks by paginating                              |
| [INLONG-8490](https://github.com/apache/inlong/issues/8490) | [Improve][Manager] Duplicate queried audit data according to all fields                                                         |
| [INLONG-8488](https://github.com/apache/inlong/issues/8488) | [INLONG-8374][Manager] Manager client tools support multiple tenant                                                             |
| [INLONG-8448](https://github.com/apache/inlong/issues/8448) | [INLONG-8447][Manager] Optimize paging logic                                                                                    |
| [INLONG-8447](https://github.com/apache/inlong/issues/8447) | [Improve][Manager] Optimize paging logic  stage/stale                                                                           |
| [INLONG-8446](https://github.com/apache/inlong/issues/8446) | [Feature][Manager] Remove the permission check logic in Services and DAOs  good first issue                                     |
| [INLONG-8403](https://github.com/apache/inlong/issues/8403) | [Feature][Manager] Support resource migrate to different tenant                                                                 |
| [INLONG-8374](https://github.com/apache/inlong/issues/8374) | [Feature][Manager] Manager client tools support multiple tenant good first issue                                                |
| [INLONG-8360](https://github.com/apache/inlong/issues/8360) | [Improve][Manager] Support previewing data of Kafka                                                                             |

### SDK
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8916](https://github.com/apache/inlong/issues/8916) | [Improve][SDK] Update SDK configuration file for dataproxy cpp sdk                                                              |
| [INLONG-8910](https://github.com/apache/inlong/issues/8910) | [Improve][SDK] Adjust some default configuration parameters                                                                     |
| [INLONG-8905](https://github.com/apache/inlong/issues/8905) | [Improve][SDK] Code specifications for dataproxy cpp sdk                                                                        |
| [INLONG-8896](https://github.com/apache/inlong/issues/8896) | [Improve][SDK] Remove old code before refactoring for dataproxy cpp sdk                                                         |
| [INLONG-8891](https://github.com/apache/inlong/issues/8891) | [Improve][SDK] Optimize compile for dataproxy cpp sdk                                                                           |
| [INLONG-8889](https://github.com/apache/inlong/issues/8889) | [Improve][SDK] Optimize CmakeList for dataproxy cpp sdk                                                                         |
| [INLONG-8887](https://github.com/apache/inlong/issues/8887) | [Improve][SDK] Optimize api framework for dataproxy cpp sdk                                                                     |
| [INLONG-8885](https://github.com/apache/inlong/issues/8885) | [Improve][SDK] Optimize tcp manager framework for dataproxy cpp sdk                                                             |
| [INLONG-8883](https://github.com/apache/inlong/issues/8883) | [Improve][SDK] Optimize proxy config manager framework for dataproxy cpp sdk                                                    |
| [INLONG-8868](https://github.com/apache/inlong/issues/8868) | [Improve][SDK] Optimize send data framework for dataproxy cpp sdk                                                               |
| [INLONG-8866](https://github.com/apache/inlong/issues/8866) | [Improve][SDK] Optimize data receiving framework for dataproxy cpp sdk                                                          |
| [INLONG-8864](https://github.com/apache/inlong/issues/8864) | [Improve][SDK] Add memory utils for dataproxy cpp sdk                                                                           |
| [INLONG-8861](https://github.com/apache/inlong/issues/8861) | [Improve][SDK] Add mutex utils for dataproxy cpp sdk                                                                            |
| [INLONG-8860](https://github.com/apache/inlong/issues/8860) | [Improve][SDK] Add log utils for dataproxy cpp sdk                                                                              |
| [INLONG-8858](https://github.com/apache/inlong/issues/8858) | [Improve][SDK] Add init helper information for dataproxy cpp sdk                                                                |
| [INLONG-8856](https://github.com/apache/inlong/issues/8856) | [Improve][SDK] Add msg information for dataproxy cpp sdk                                                                        |
| [INLONG-8854](https://github.com/apache/inlong/issues/8854) | [Improve][SDK] Add return code information for dataproxy cpp sdk                                                                |
| [INLONG-8784](https://github.com/apache/inlong/issues/8784) | [INLONG-8766][SDK] SortSdk create consumer in parallel                                                                          |
| [INLONG-8766](https://github.com/apache/inlong/issues/8766) | [Improve][SDK] SortSdk create consumer in parallel                                                                              |
| [INLONG-8747](https://github.com/apache/inlong/issues/8747) | [Improve][SDK] Optimize the local configuration management of cpp sdk                                                           |
| [INLONG-8728](https://github.com/apache/inlong/issues/8728) | [Improve][SDK] Optimize the problem of third-party openssl library dependency failure                                           |
| [INLONG-8639](https://github.com/apache/inlong/issues/8639) | [Improve][SDK] Improve send failed logic of DataProxy                                                                           |
| [INLONG-8637](https://github.com/apache/inlong/issues/8637) | [Improve][SDK] Pool data request and batch request for DataProxy                                                                |
| [INLONG-8635](https://github.com/apache/inlong/issues/8635) | [Improve][SDK] Update dependency packages and required Go version for DataProxy                                                 |
| [INLONG-8633](https://github.com/apache/inlong/issues/8633) | [Improve][SDK] Update debug log level for DataProxy SDK                                                                         |
| [INLONG-8631](https://github.com/apache/inlong/issues/8631) | [Improve][SDK] Handle context.Done() in Send() for DataProxy SDK                                                                |

### Sort
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8897](https://github.com/apache/inlong/issues/8897) | [Bug][Sort] update dbz option name 'schema. whitelist' to 'schema.include.list'                                                 |
| [INLONG-8848](https://github.com/apache/inlong/issues/8848) | [Bug][Sort] Sort base dependency error                                                                                          |
| [INLONG-8839](https://github.com/apache/inlong/issues/8839) | [Feature][Sort] Add audit metric in starrocks connector on flink 1.15                                                           |
| [INLONG-8838](https://github.com/apache/inlong/issues/8838) | [Improve][Sort] IcebergSource support metadata                                                                                  |
| [INLONG-8825](https://github.com/apache/inlong/issues/8825) | [Feature][Sort] Optimize the field type conversion between source and target in the whole database scenario                     |
| [INLONG-8818](https://github.com/apache/inlong/issues/8818) | [INLONG-8643][Sort] Support Iceberg source                                                                                      |
| [INLONG-8808](https://github.com/apache/inlong/issues/8808) | [Improve][Sort] Improve convertToBinary for RowDataDebeziumDeserializeSchema                                                    |
| [INLONG-8796](https://github.com/apache/inlong/issues/8796) | [Improve][Sort] Add SchemaChangeEventHandler to deal schema change event by each connector                                      |
| [INLONG-8786](https://github.com/apache/inlong/issues/8786) | [Bug][Sort] The Doris schema should be changed in case of multiple URI                                                          |
| [INLONG-8783](https://github.com/apache/inlong/issues/8783) | [Bug][Sort] MySql connector jdbc version is incompatible with mysql-cdc version                                                 |
| [INLONG-8776](https://github.com/apache/inlong/issues/8776) | [Improve][Sort] MySql connector should add jdbc driver jar                                                                      |
| [INLONG-8745](https://github.com/apache/inlong/issues/8745) | [Improve][Sort] Add incremental and postgre type in postgre connector                                                           |
| [INLONG-8743](https://github.com/apache/inlong/issues/8743) | [Feature][Sort] Support more type of ddl in all migration                                                                       |
| [INLONG-8667](https://github.com/apache/inlong/issues/8667) | [Improve][Sort] The inner class name was wrong in OracleSnapshotContext                                                         |
| [INLONG-8653](https://github.com/apache/inlong/issues/8653) | [Bug][Sort] fix the query sql for jdbc pg dialect multiple table scenerios.                                                     |
| [INLONG-8643](https://github.com/apache/inlong/issues/8643) | [Feature][Sort] Add Iceberg source on flink 1.15                                                                                |
| [INLONG-8641](https://github.com/apache/inlong/issues/8641) | [Bug][Sort] SingleTableCustomFieldsPartitioner package name does not match path                                                 |
| [INLONG-8616](https://github.com/apache/inlong/issues/8616) | [INLONG-8598][Sort] Optimize sortstandalone pulsar sink                                                                         |
| [INLONG-8602](https://github.com/apache/inlong/issues/8602) | [Bug][Sort] Fix StackOverflowError of Oracle CDC                                                                                |
| [INLONG-8598](https://github.com/apache/inlong/issues/8598) | [Improve][Sort] Optimize sortstandalone pulsar sink                                                                             |
| [INLONG-8596](https://github.com/apache/inlong/issues/8596) | [Feature][Sort] Iceberg supports dynamic switching between append and upsert                                                    |
| [INLONG-8594](https://github.com/apache/inlong/issues/8594) | [Bug][Sort] When change record is chunk range of snapshot phase, MongoDB cannot rewrite the record                              |
| [INLONG-8579](https://github.com/apache/inlong/issues/8579) | [INLONG-8578][Sort] Fix npe inside outputReadPhaseMetrics in mysql-cdc                                                          |
| [INLONG-8578](https://github.com/apache/inlong/issues/8578) | [Bug][Sort] NPE occurred inside outputReadPhaseMetrics of mysql-cdc                                                             |
| [INLONG-8558](https://github.com/apache/inlong/issues/8558) | [Improve][Sort] Use database name in upper case at the OracleTableSourceFactory                                                 |
| [INLONG-8551](https://github.com/apache/inlong/issues/8551) | [INLONG-8549][Sort] Fix incorrect use of maven plugin on integration test among sort-end-to-end-tests                           |
| [INLONG-8549](https://github.com/apache/inlong/issues/8549) | [Bug][Sort] Incorrect use of maven plugin on integration test among sort-end-to-end-tests                                       |
| [INLONG-8445](https://github.com/apache/inlong/issues/8445) | [Feature][Sort] Support running tests on both Flink 1.13 and Flink 1.15                                                         |
| [INLONG-8436](https://github.com/apache/inlong/issues/8436) | [Bug][Sort] The backfill task not running bug in oracle cdc connector                                                           |
| [INLONG-8279](https://github.com/apache/inlong/issues/8279) | [Bug][Sort] NPE when run MySqlLoadSqlParseTest                                                                                  |
| [INLONG-8236](https://github.com/apache/inlong/issues/8236) | [Feature][Sort] Iceberg supports dynamic switching between append and upsert                                                    |
| [INLONG-7908](https://github.com/apache/inlong/issues/7908) | [Feature][Sort] PostgreSQL connector supports parallel read                                                                     |
| [INLONG-7900](https://github.com/apache/inlong/issues/7900) | [Feature][Sort] Support partition by custom fields when upsert single table of Kafka                                            |
| [INLONG-7763](https://github.com/apache/inlong/issues/7763) | [Feature][Sort] Support ddl change for doris                                                                                    |
| [INLONG-8797](https://github.com/apache/inlong/issues/8797) | [Feature][Manager][Sort] Audit has no data for data sync                                                                        |
| [INLONG-8903](https://github.com/apache/inlong/issues/8903) | [Bug][TubeMQ][Sort] int64 not recognized by the compiler and Missing @Override annotations                                      |

### TubeMQ
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-4972](https://github.com/apache/inlong/issues/4972) | [Feature][TubeMQ] Add TubeMQ Command Tools stage/roadmap                                                                        |
| [INLONG-8903](https://github.com/apache/inlong/issues/8903) | [Bug][TubeMQ][Sort] int64 not recognized by the compiler and Missing @Override annotations                                      |
| [INLONG-8871](https://github.com/apache/inlong/issues/8871) | [Improve][TubeMQ] Use an error code in checkMessageAndStatus() to return the check result instead of throwing an exception      |
| [INLONG-8812](https://github.com/apache/inlong/issues/8812) | [Improve][Tubemq] Missing parameter                                                                                             |
| [INLONG-8793](https://github.com/apache/inlong/issues/8793) | [INLONG-8791][TubeMQ] Tubemq-client-go lacks log level configuration API                                                        |
| [INLONG-8768](https://github.com/apache/inlong/issues/8768) | [Improve][TubeMQ] Adding restart-manager.sh for inlong-tubemq-manager                                                           |
| [INLONG-8720](https://github.com/apache/inlong/issues/8720) | [Improve][TubeMQ] Some unused return params in WebParamaterUtils                                                                |
| [INLONG-8717](https://github.com/apache/inlong/issues/8717) | [Bug][TubeMQ] Display wrong response message in TubeMQ master web panel                                                         |
| [INLONG-8716](https://github.com/apache/inlong/issues/8716) | [Bug][TubeMQ] set error code 0 when it works                                                                                    |
| [INLONG-8701](https://github.com/apache/inlong/issues/8701) | [Improve][TubeMQ] Master dashboard respond always true when some operation complete                                             |

### Other
|                            ISSUE                            | Summary                                                                                                                         |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8614](https://github.com/apache/inlong/issues/8614) | [Bug][CI] Post maven cache failed for UT and build workflow service/ci                                                          |
| [INLONG-8494](https://github.com/apache/inlong/issues/8494) | [Bug][CI] Caught IOException "No space left on device" for Analyze by CodeQL workflow                                           |
| [INLONG-8846](https://github.com/apache/inlong/issues/8846) | [Improve][Doc] Add ASF DOAP File for InLong                                                                                     |
| [INLONG-8543](https://github.com/apache/inlong/issues/8543) | [Bug][Docker] The path of the mysql connector is wrong when docker build service/docker                                         |
| [INLONG-8533](https://github.com/apache/inlong/issues/8533) | [Improve][Docker] Add MySQL connector to the manager image service/docker                                                       |
| [INLONG-8918](https://github.com/apache/inlong/issues/8918) | [Bug][Script] A script parameter error in bin/inlong-daemon                                                                     |
| [INLONG-8690](https://github.com/apache/inlong/issues/8690) | [Improve][Security] TemporaryFolder on unix-like systems does not limit access to created files                                 |
| [INLONG-8688](https://github.com/apache/inlong/issues/8688) | [Improve][Security] Incorrect Authorization in MySQL Connector Java                                                             |
| [INLONG-8687](https://github.com/apache/inlong/issues/8687) | [Improve][Security] Apache Pulsar Java Client vulnerable to Improper Certificate Validation                                     |
| [INLONG-8686](https://github.com/apache/inlong/issues/8686) | [Improve][Security] TemporaryFolder on unix-like systems does not limit access to created files                                 |
| [INLONG-8685](https://github.com/apache/inlong/issues/8685) | [Improve][Security] Denial of service due to parser crash                                                                       |
| [INLONG-8684](https://github.com/apache/inlong/issues/8684) | [Improve][Security] Vite Server Options (server.fs.deny) can be bypassed using double forward-slash (//)                        |
| [INLONG-8683](https://github.com/apache/inlong/issues/8683) | [Improve][Security] OutOfMemoryError for large multipart without filename in Eclipse Jetty                                      |
| [INLONG-8682](https://github.com/apache/inlong/issues/8682) | [Improve][Security] Guava vulnerable to insecure use of temporary directory                                                     |
| [INLONG-8681](https://github.com/apache/inlong/issues/8681) | [Improve][Security] netty-handler SniHandler 16MB allocation                                                                    |
