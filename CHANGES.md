
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

# Release InLong 2.1.0 - Released (as of 2025-01-03)
### Agent
|ISSUE|Summary|
|:--|:--|
|[INLONG-11413](https://github.com/apache/inlong/issues/11413)|[Improve][Agent] By default, Agent status and file metrics are not reported |
|[INLONG-11451](https://github.com/apache/inlong/issues/11451)|[Improve][Agent] When the installer detects that the process does not exist, it increases the wait for retry to prevent misjudgment |
|[INLONG-11461](https://github.com/apache/inlong/issues/11461)|[Improve][Agent] HeartbeatManager does not create a DefaultMessageSender |
|[INLONG-11499](https://github.com/apache/inlong/issues/11499)|[Improve][Agent] By default, use the locally configured audit address |
|[INLONG-11506](https://github.com/apache/inlong/issues/11506)|[Improve][Agent] Task start and end time using string type |
|[INLONG-11516](https://github.com/apache/inlong/issues/11516)|[Improve][Agent] Accelerate the process exit speed |
|[INLONG-11518](https://github.com/apache/inlong/issues/11518)|[Improve][Agent] Support multiple processes |
|[INLONG-11522](https://github.com/apache/inlong/issues/11522)|[Improve][Agent] Strictly process new instances in the order of submission |
|[INLONG-11524](https://github.com/apache/inlong/issues/11524)|[Improve][Agent] Save offset before exiting to reduce data duplication |
|[INLONG-11527](https://github.com/apache/inlong/issues/11527)|[Improve][Agent] Save both row and byte position information when saving offset |
|[INLONG-11529](https://github.com/apache/inlong/issues/11529)|[Improve][Agent] Add exception handling for audit SDK |
|[INLONG-11556](https://github.com/apache/inlong/issues/11556)|[Improve][Agent] Resolve exceptions when saving installation packages |
|[INLONG-11558](https://github.com/apache/inlong/issues/11558)|[Improve][Agent] Modify the default cluster label of the configuration file |
|[INLONG-11560](https://github.com/apache/inlong/issues/11560)|[Improve][Agent] Adjust the directory of data source extension plugins |
|[INLONG-11562](https://github.com/apache/inlong/issues/11562)|[Improve][Agent] Modify the MemoryManager class to support adding semaphore
|[INLONG-11569](https://github.com/apache/inlong/issues/11569)|[Improve][Agent] Add COS Task |
|[INLONG-11569](https://github.com/apache/inlong/issues/11569)|[Improve][Agent] Add COS Task
|[INLONG-11571](https://github.com/apache/inlong/issues/11571)|[Improve][Agent] Add classes for actual collection of COS source |
|[INLONG-11571](https://github.com/apache/inlong/issues/11571)|[Improve][Agent] Add classes for actual collection of COS source |
|[INLONG-11574](https://github.com/apache/inlong/issues/11574)|[Improve][Agent] Add COS source unit test
|[INLONG-11591](https://github.com/apache/inlong/issues/11591)|[Improve][Agent] Reduce duplicate code for log collection type tasks |
|[INLONG-11591](https://github.com/apache/inlong/issues/11591)|[Improve][Agent] Reduce duplicate code for log collection type tasks |
|[INLONG-11614](https://github.com/apache/inlong/issues/11614)|[Improve][Agent] The AbstractSource class poses a risk of semaphore leakage
|[INLONG-11624](https://github.com/apache/inlong/issues/11624)|[Improve][Agent] Add SQL data source |

### Dashboard
|ISSUE|Summary|
|:--|:--|
|[INLONG-11643](https://github.com/apache/inlong/issues/11643)|[Feature][Dashboard] The MySQL node password is not required |
|[INLONG-11186](https://github.com/apache/inlong/issues/11186)|[Improve][Dashboard] Export audit indicator data to csv |
|[INLONG-11186](https://github.com/apache/inlong/issues/11186)|[Improve][Dashboard] Export audit indicator data to csv |
|[INLONG-11187](https://github.com/apache/inlong/issues/11187)|[Improve][Dashboard] Agent batch upgrade |
|[INLONG-11425](https://github.com/apache/inlong/issues/11425)|[Improve][Dashboard] Data preview body field is not differentiated |
|[INLONG-11425](https://github.com/apache/inlong/issues/11425)|[Improve][Dashboard] Data preview body field is not differentiated |
|[INLONG-11441](https://github.com/apache/inlong/issues/11441)|[Improve][Dashboard] Add ClusterTag filtering conditions to the cluster interface
|[INLONG-11449](https://github.com/apache/inlong/issues/11449)|[Improve][Dashboard] Add clear and search functions to some drop-down boxes |
|[INLONG-11449](https://github.com/apache/inlong/issues/11449)|[Improve][Dashboard] Add clear and search functions to some drop-down boxes |
|[INLONG-11465](https://github.com/apache/inlong/issues/11465)|[Improve][Dashboard] The sink drop-down box supports search |

### Manager
|ISSUE|Summary|
|:--|:--|
|[INLONG-11361](https://github.com/apache/inlong/issues/11361)|[Improve][Manager] Support querying heartbeat information based on IP address |
|[INLONG-11368](https://github.com/apache/inlong/issues/11368)|[Improve][Manager] Determine whether to issue a streamSource based on the stream status |
|[INLONG-11375](https://github.com/apache/inlong/issues/11375)|[Improve][Manager] Add a restriction that an IP can only belong to one cluster |
|[INLONG-11377](https://github.com/apache/inlong/issues/11377)|[Improve][Manager] Add verification for oceanusBase URL |
|[INLONG-11387](https://github.com/apache/inlong/issues/11387)|[Improve][Manager] Support multi-threaded processing agent installation |
|[INLONG-11389](https://github.com/apache/inlong/issues/11389)|[Improve][Manager] Installation log display operation time |
|[INLONG-11391](https://github.com/apache/inlong/issues/11391)|[Improve][Manager] Add openAPI for adding data add tasks |
|[INLONG-11397](https://github.com/apache/inlong/issues/11397)|[Improve][Manager] Support copy modules.json when installer reinstall |
|[INLONG-11402](https://github.com/apache/inlong/issues/11402)|[Improve][Manager] Download the installer to a temporary directory |
|[INLONG-11415](https://github.com/apache/inlong/issues/11415)|[Improve][Manager] Support querying cluster node operation records |
|[INLONG-11433](https://github.com/apache/inlong/issues/11433)|[Improve][Manager] Add validation for the parameter advertising Address |
|[INLONG-11487](https://github.com/apache/inlong/issues/11487)|[Improve][Manager] Support adding data add tasks based on the source ID |
|[INLONG-11508](https://github.com/apache/inlong/issues/11508)|[Improve][Manager] Add APIs to dirty data query |
|[INLONG-11513](https://github.com/apache/inlong/issues/11513)|[Improve][Manager] Rename configuration names related to dirty data |
|[INLONG-11533](https://github.com/apache/inlong/issues/11533)|[Improve][Manager] Enable Manager to use multiple scheduling engines simultaneously |
|[INLONG-11551](https://github.com/apache/inlong/issues/11551)|[Improve][Manager] The interface does not return scheduleEngine, causing the page echo to fail. |
|[INLONG-11567](https://github.com/apache/inlong/issues/11567)|[Improve][Manager] Optimize the original DAG of Airflow |
|[INLONG-11585](https://github.com/apache/inlong/issues/11585)|[Improve][Manager] Support JDBC verification under dual write parameters |
|[INLONG-11608](https://github.com/apache/inlong/issues/11608)|[Improve][Manager] Add permission verification for ordinary users to create streams and sinks |
|[INLONG-11618](https://github.com/apache/inlong/issues/11618)|[Improve][Manager] Support COS stream source |
|[INLONG-11342](https://github.com/apache/inlong/issues/11342)|[Feature][Manager] Support Transform Function doc display |
|[INLONG-11401](https://github.com/apache/inlong/issues/11401)|[Feature][Manager] Support Dolphinscheduler schedule engine |
|[INLONG-11400](https://github.com/apache/inlong/issues/11400)|[Feature][Manager] Support Airflow schedule engine |
|[INLONG-11483](https://github.com/apache/inlong/issues/11483)|[Feature][Manager] Support multiple schedule engine |
|[INLONG-11531](https://github.com/apache/inlong/issues/11531)|[Feature][Manager] Fix bug in DolphinScheduler engine |
|[INLONG-11535](https://github.com/apache/inlong/issues/11535)|[Feature][Manager] Enhance schedule engine config |
|[INLONG-11366](https://github.com/apache/inlong/issues/11366)|[Bug][Manager] Data preview error handling line delimiter |
|[INLONG-11412](https://github.com/apache/inlong/issues/11412)|[Bug][Manager] Failed to handle request on path: /inlong/manager/openapi/dataproxy/getIpList/inlong_agent_system by user: admin |

### SDK
|ISSUE|Summary|
|:--|:--|
|[INLONG-10873](https://github.com/apache/inlong/issues/10873)|[Feature][SDK] Transform support factorial function |
|[INLONG-11002](https://github.com/apache/inlong/issues/11002)|[Feature][SDK] Transform SQL support Fibonacci function |
|[INLONG-11051](https://github.com/apache/inlong/issues/11051)|[Feature][SDK] Transform SQL supports parsing of in & any & some & all |
|[INLONG-11302](https://github.com/apache/inlong/issues/11302)|[Feature][SDK] Transform SQL supports "JSON_INSERT" function |
|[INLONG-11303](https://github.com/apache/inlong/issues/11303)|[Feature][SDK] Transform SQL supports "JSON_REMOVE" function |
|[INLONG-11304](https://github.com/apache/inlong/issues/11304)|[Feature][SDK] Transform SQL supports "JSON_REPLACE" function |
|[INLONG-11305](https://github.com/apache/inlong/issues/11305)|[Feature][SDK] Transform SQL supports "JSON_SET" function |
|[INLONG-11372](https://github.com/apache/inlong/issues/11372)|[Feature][SDK] LocalTimeFunctionTest may fail in some situation |
|[INLONG-11382](https://github.com/apache/inlong/issues/11382)|[Feature][SDK] Optimize all columns select of Transform SDK |
|[INLONG-11546](https://github.com/apache/inlong/issues/11546)|[Feature][SDK] Support async and sync report dirty data |
|[INLONG-11611](https://github.com/apache/inlong/issues/11611)|[Feature][SDK] Transform SDK supports RowData source and sink |
|[INLONG-11616](https://github.com/apache/inlong/issues/11616)|[Feature][SDK] Use self-defined Field and RowData conversion utils |
|[INLONG-11352](https://github.com/apache/inlong/issues/11352)|[Improve][SDK] Add dirty data collection sdk |
|[INLONG-11426](https://github.com/apache/inlong/issues/11426)|[Improve][SDK] Optimize dirty data sdk |
|[INLONG-11457](https://github.com/apache/inlong/issues/11457)|[Improve][SDK] Optimize SequentialID class implementation |
|[INLONG-11459](https://github.com/apache/inlong/issues/11459)|[Improve][SDK] Add MetricConfig class to save metric-related settings |
|[INLONG-11463](https://github.com/apache/inlong/issues/11463)|[Improve][SDK] Remove deprecated APIs in the DefaultMessageSender class |
|[INLONG-11469](https://github.com/apache/inlong/issues/11469)|[Improve][SDK] Optimize the single message processing logic in the EncodeObject class |
|[INLONG-11475](https://github.com/apache/inlong/issues/11475)|[Improve][SDK] Remove the timeout parameter in the MessageSender class functions |
|[INLONG-11493](https://github.com/apache/inlong/issues/11493)|[Improve][SDK] Inlong SDK Dirty Sink supports retry sending |
|[INLONG-11520](https://github.com/apache/inlong/issues/11520)|[Improve][SDK] Remove DirtyServerType, use SinkType |
|[INLONG-11565](https://github.com/apache/inlong/issues/11565)|[Improve][SDK] Optimize the implementation of the Utils.java class |
|[INLONG-11580](https://github.com/apache/inlong/issues/11580)|[Improve][SDK] Remove the Manager address fetche logic |
|[INLONG-11589](https://github.com/apache/inlong/issues/11589)|[Improve][SDK] Optimize the implementation of proxy configuration management |
|[INLONG-11595](https://github.com/apache/inlong/issues/11595)|[Improve][SDK] Optimize the implementation of node connection management |
|[INLONG-11597](https://github.com/apache/inlong/issues/11597)|[Improve][SDK] Optimize the generation speed of UUIDv4 for Golang SDK |
|[INLONG-11599](https://github.com/apache/inlong/issues/11599)|[Improve][SDK] Optimize the configuration related content in the ProxyClientConfig class |
|[INLONG-11629](https://github.com/apache/inlong/issues/11629)|[Improve][SDK] Adjust the Sender initialization logic |
|[INLONG-11453](https://github.com/apache/inlong/issues/11453)|[Bug][SDK] notifyHBControl function array out of bounds |
|[INLONG-11491](https://github.com/apache/inlong/issues/11491)|[Bug][SDK] Fix the judgment error of onMessageAck function in MetricSendCallBack class |
|[INLONG-11584](https://github.com/apache/inlong/issues/11584)|[Bug][SDK] Panic caused by nil pointer reference when calling dataproxy.(*client).OnClose |
|[INLONG-11632](https://github.com/apache/inlong/issues/11632)|[Bug][SDK] The manager address was mistakenly truncated |

### Sort
|ISSUE|Summary|
|:--|:--|
|[INLONG-10467](https://github.com/apache/inlong/issues/10467)|[Feature][Sort] Add Elasticsearch connector on Flink 1.18 |
|[INLONG-11340](https://github.com/apache/inlong/issues/11340)|[Feature][Sort] Add new source metrics for sort-connector-pulsar-v1.15 |
|[INLONG-11355](https://github.com/apache/inlong/issues/11355)|[Feature][Sort] Add new source metrics for sort-connector-mongodb-cdc-v1.15 |
|[INLONG-11357](https://github.com/apache/inlong/issues/11357)|[Feature][Sort] Add new source metrics for sort-connector-sqlserver-cdc-v1.15 |
|[INLONG-11481](https://github.com/apache/inlong/issues/11481)|[Feature][Sort] Tube Connector source supports dirty data achieving |
|[INLONG-11576](https://github.com/apache/inlong/issues/11576)|[Feature][Sort] Support KV separator for kafka sink |
|[INLONG-11407](https://github.com/apache/inlong/issues/11407)|[Improve][Sort] Remove generated code for InLongBinlog |
|[INLONG-11537](https://github.com/apache/inlong/issues/11537)|[Improve][Sort] Optimize the session key generation of TubeMQ Source |
|[INLONG-11369](https://github.com/apache/inlong/issues/11369)|[Bug][Sort] KV split has error when there is a escape char without before & and = in text |
|[INLONG-11422](https://github.com/apache/inlong/issues/11422)|[Bug][Sort] redundant&conflicts dependencys of OpenTelemetryLogger |
|[INLONG-11455](https://github.com/apache/inlong/issues/11455)|[Bug][Sort] Only one OpenTelemetryAppender should be registered |
|[INLONG-11455](https://github.com/apache/inlong/issues/11455)|[Bug][Sort] Only one OpenTelemetryAppender should be registered
|[INLONG-11473](https://github.com/apache/inlong/issues/11473)|[Bug][Sort] AUDIT_DATA_TIME not removed when sink to StarRocks |

### Audit
|ISSUE|Summary|
|:--|:--|
|[INLONG-11320](https://github.com/apache/inlong/issues/11320)|[Improve][Audit] Add a metric monitoring system for the Audit Proxy itself |
|[INLONG-11360](https://github.com/apache/inlong/issues/11360)|[Improve][Audit] Add a metric monitoring system for the Audit Store itself |
|[INLONG-11364](https://github.com/apache/inlong/issues/11364)|[Improve][Audit] Add a metric monitoring system for the Audit Service itself |
|[INLONG-11393](https://github.com/apache/inlong/issues/11393)|[Improve][Audit] Optimize indicator statistics key for Audit |
|[INLONG-11396](https://github.com/apache/inlong/issues/11396)|[Improve][Audit] Add monitoring of pulsar producer creation failure for Audit Proxy |
|[INLONG-11404](https://github.com/apache/inlong/issues/11404)|[Improve][Audit] Optimize the Audit item in dashboard display |
|[INLONG-11406](https://github.com/apache/inlong/issues/11406)|[Improve][Audit] Provides an interface for asynchronously flushing Audit data |
|[INLONG-11442](https://github.com/apache/inlong/issues/11442)|[Improve][Audit] Optimize the exception log of Audit SDK |
|[INLONG-11489](https://github.com/apache/inlong/issues/11489)|[Improve][Audit] Use Throwable instead of Exception to capture Java package conflicts |
|[INLONG-11605](https://github.com/apache/inlong/issues/11605)|[Improve][Audit] Added audit data legitimacy verification |

### TubeMQ
|ISSUE|Summary|
|:--|:--|
|[INLONG-11386](https://github.com/apache/inlong/issues/11386)|[Improve][TubeMQ] Use local files to save consumer group offset information |
|[INLONG-11446](https://github.com/apache/inlong/issues/11446)|[Improve][TubeMQ] Remove legacy codes |
|[INLONG-11583](https://github.com/apache/inlong/issues/11583)|[Bug][TubeMQ] Go SDK load balance logic not perfect, causing consumption to stop suddenly |
|[INLONG-11587](https://github.com/apache/inlong/issues/11587)|[Bug][TubeMQ] Go SDK change filter order cause inconsistency error when registering to master. |

### Others
|ISSUE|Summary|
|:--|:--|
|[INLONG-11541](https://github.com/apache/inlong/issues/11541)|[Feature][Docker] Support Manager config volume |
|[INLONG-11246](https://github.com/apache/inlong/issues/11246)|[Feature][CI] Remove dangling docker images |
|[INLONG-11439](https://github.com/apache/inlong/issues/11439)|[Improve][CI] Support parallel build |
|[INLONG-11471](https://github.com/apache/inlong/issues/11471)|[Improve][CI] Workflow may fail when building the project accidentally|
|[INLONG-11444](https://github.com/apache/inlong/issues/11444)|[Bug][Distribution] Merging modules jars is not working |