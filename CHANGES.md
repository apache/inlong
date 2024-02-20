
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

# Release InLong 1.11.0 - Released (as of 2024-02-04)
### Agent
|                            ISSUE                            | Summary                                                                                                 |
|:-----------------------------------------------------------:|:--------------------------------------------------------------------------------------------------------|
| [INLONG-9457](https://github.com/apache/inlong/issues/9457) | [Improve][Agent] Add task and instance heartbeat audit                                                  |
| [INLONG-9481](https://github.com/apache/inlong/issues/9481) | [Improve][Agent] Add unit test of reading with offset                                                   |
| [INLONG-9580](https://github.com/apache/inlong/issues/9580) | [Improve][Agent] Add unit testing to taskmanager to test their ability to recover tasks from DB         |
| [INLONG-9614](https://github.com/apache/inlong/issues/9614) | [Improve][Agent] Change folder name                                                                     |
| [INLONG-9467](https://github.com/apache/inlong/issues/9467) | [Improve][Agent] Improve code exception detection to ensure task and instance state transitions         |
| [INLONG-9454](https://github.com/apache/inlong/issues/9454) | [Improve][Agent] Increase exit conditions to prevent dead loops                                         |
| [INLONG-9556](https://github.com/apache/inlong/issues/9556) | [Improve][Agent] Prevent thread freeze caused by deleting data sources when the backend cannot send out |
| [INLONG-9572](https://github.com/apache/inlong/issues/9572) | [Improve][Agent] Set data time of message cache by sink data time                                       |
| [INLONG-9548](https://github.com/apache/inlong/issues/9548) | [Improve][Agent] Supports HTTPS and can determine whether to enable it through configuration            |
| [INLONG-9600](https://github.com/apache/inlong/issues/9600) | [Improve][Agent]Adjust the sinks directory for code consistency                                         |

### Dashboard
|                            ISSUE                            | Summary                                                                                                      |
|:-----------------------------------------------------------:|:-------------------------------------------------------------------------------------------------------------|
| [INLONG-9627](https://github.com/apache/inlong/issues/9627) | [Improve][Dashboard] Audit and transmission delay filtering query optimization                               |
| [INLONG-9543](https://github.com/apache/inlong/issues/9543) | [Improve][Dashboard] Cls, Pulsar and es sink support saving sortTaskName and sortConsumerGroup               |
| [INLONG-9610](https://github.com/apache/inlong/issues/9610) | [Improve][Dashboard] Cluster creation type optimization                                                      |
| [INLONG-9495](https://github.com/apache/inlong/issues/9495) | [Improve][Dashboard] Data synchronization basic information page optimization                                |
| [INLONG-9663](https://github.com/apache/inlong/issues/9663) | [Improve][Dashboard] Data synchronization doris sink supports append Mode                                    |
| [INLONG-9530](https://github.com/apache/inlong/issues/9530) | [Improve][Dashboard] Dataproxy cluster nodes support adding the enabledOnline parameter                      |
| [INLONG-9505](https://github.com/apache/inlong/issues/9505) | [Improve][Dashboard] Pulsar source parameter optimization                                                    |
| [INLONG-9516](https://github.com/apache/inlong/issues/9516) | [Improve][Dashboard] Remove useless dependencies                                                             |
| [INLONG-8393](https://github.com/apache/inlong/issues/8393) | [Improve][DataProxy] Optimize the HeartbeatManager class                                                     |
| [INLONG-9657](https://github.com/apache/inlong/issues/9657) | [Feature][Dashboard] Support management of Doris data node                                                   |
| [INLONG-9439](https://github.com/apache/inlong/issues/9439) | [Feature][Dashboard] Support module reconciliation function                                                  |
| [INLONG-9550](https://github.com/apache/inlong/issues/9550) | [Feature][Dashboard] System operation and maintenance supports query and audit through Gorupid and Stream id |
| [INLONG-9489](https://github.com/apache/inlong/issues/9489) | [Bug][Dashboard] Transform remove missing stream id                                                          |

### Manager
|                            ISSUE                            | Summary                                                                                                                                       |
|:-----------------------------------------------------------:|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-9508](https://github.com/apache/inlong/issues/9508) | [Feature][Manager] Add Iceberg field type mapping strategy to improve usability                                                               |
| [INLONG-9634](https://github.com/apache/inlong/issues/9634) | [Feature][Manager] Auto assgin sort task and consumer group of standalone export                                                              |
| [INLONG-9056](https://github.com/apache/inlong/issues/9056) | [Feature][Manager] Configuration change detection                                                                                             |
| [INLONG-9524](https://github.com/apache/inlong/issues/9524) | [Feature][Manager] Manager client support migrate group tenant                                                                                |
| [INLONG-9452](https://github.com/apache/inlong/issues/9452) | [Improve][Manager] Add audit items for file agent                                                                                             |
| [INLONG-9484](https://github.com/apache/inlong/issues/9484) | [Improve][Manager] Improve logic of sortstandalone sink auto-assigned cluster                                                                 |
| [INLONG-9441](https://github.com/apache/inlong/issues/9441) | [Improve][Manager] MySQL data source supports both full and incremental modes                                                                 |
| [INLONG-8392](https://github.com/apache/inlong/issues/8392) | [Improve][Manager] Optimize the HeartbeatMsg class                                                                                            |
| [INLONG-9586](https://github.com/apache/inlong/issues/9586) | [Improve][Manager] Provide installation agent framework                                                                                       |
| [INLONG-9528](https://github.com/apache/inlong/issues/9528) | [Improve][Manager] Support configuring the switch to enable dataproxy nodes                                                                   |
| [INLONG-9591](https://github.com/apache/inlong/issues/9591) | [Improve][Manager] Support printing thread status before submitting tasks                                                                     |
| [INLONG-9523](https://github.com/apache/inlong/issues/9523) | [Improve][Manager] Support querying all audit information based on IP address                                                                 |
| [INLONG-9440](https://github.com/apache/inlong/issues/9440) | [Improve][Manager] Support querying audit information based on ip                                                                             |
| [INLONG-9518](https://github.com/apache/inlong/issues/9518) | [Improve][Manager] Support resetting the consumption location of the consumption group used by sort                                           |
| [INLONG-9475](https://github.com/apache/inlong/issues/9475) | [Improve][Manager] Support setting dataNode when configuring streamSource for MYSQL                                                           |
| [INLONG-9533](https://github.com/apache/inlong/issues/9533) | [Improve][Manager] Support setting dataNode when configuring streamSource for Pulsar/Iceberg/PostgreSQL                                       |
| [INLONG-9510](https://github.com/apache/inlong/issues/9510) | [Improve][Manager] Supports doris database synchronization                                                                                    |
| [INLONG-9503](https://github.com/apache/inlong/issues/9503) | [Improve][Manager] Unified Hashmap Dependency Package                                                                                         |
| [INLONG-9577](https://github.com/apache/inlong/issues/9577) | [Bug][Manager] Datatime field type conversion error                                                                                           |
| [INLONG-9204](https://github.com/apache/inlong/issues/9204) | [Bug][Manager] Failed to use PostgreSQL sink                                                                                                  |
| [INLONG-9470](https://github.com/apache/inlong/issues/9470) | [Bug][Manager] Failed to verify if the namespace exists                                                                                       |
| [INLONG-9461](https://github.com/apache/inlong/issues/9461) | [Bug][Manager] Failure of GroupTaskListenerFactoryTest                                                                                        |
| [INLONG-9618](https://github.com/apache/inlong/issues/9618) | [Bug][Manager] HttpUtils did not process 307 status code                                                                                      |
| [INLONG-9606](https://github.com/apache/inlong/issues/9606) | [Bug][Manager] Incorrect flow status when cls sink configuration fails                                                                        |
| [INLONG-9488](https://github.com/apache/inlong/issues/9488) | [Bug][Manager] Not redirecting post requests                                                                                                  |
| [INLONG-9552](https://github.com/apache/inlong/issues/9552) | [Bug][Manager] Sink remains in configuration after standalone cluster allocation failure                                                      |
| [INLONG-9632](https://github.com/apache/inlong/issues/9632) | [Bug][Manager] StartupSortListener may miss to build flink job for every stream                                                               |
| [INLONG-9222](https://github.com/apache/inlong/issues/9222) | [Bug][Manager] When cluster have multiple cluster tag,manager return wrong metadata info                                                      |

                                                         
### SDK
|                            ISSUE                            | Summary                                                                                                               |
|:-----------------------------------------------------------:|:----------------------------------------------------------------------------------------------------------------------|
| [INLONG-9554](https://github.com/apache/inlong/issues/9554) | [Bug][SDK] When close DataProxy SDK sender thread, The shutdownInternalThreads methods throw the NullPointerException |
| [INLONG-9624](https://github.com/apache/inlong/issues/9624) | [Improve][SDK] Adjust code directory structure                                                                        |
| [INLONG-9658](https://github.com/apache/inlong/issues/9658) | [Improve][SDK] Bump up Golang SDK modules                                                                             |
| [INLONG-9546](https://github.com/apache/inlong/issues/9546) | [Feature][SDK] DataProxy SDK support request manager by https                                                         |

### Sort
|                            ISSUE                            | Summary                                                                                       |
|:-----------------------------------------------------------:|:----------------------------------------------------------------------------------------------|
| [INLONG-8358](https://github.com/apache/inlong/issues/8358) | [Feature][Sort] Add Kafka connector on Flink 1.15                                             |
| [INLONG-9521](https://github.com/apache/inlong/issues/9521) | [Feature][Sort] Sort format supports InLongMsg-kv format                                      |
| [INLONG-9571](https://github.com/apache/inlong/issues/9571) | [Feature][sort] Support row way of sort InLong message binlog format                          |
| [INLONG-9597](https://github.com/apache/inlong/issues/9597) | [Feature][Sort] Support row way of sort InLong message tlog-csv format                        |
| [INLONG-9569](https://github.com/apache/inlong/issues/9569) | [Feature][Sort] Support rowdata way of sort InLong message csv format                         |
| [INLONG-9563](https://github.com/apache/inlong/issues/9563) | [Feature][Sort] Support rowdata way of sort message kv format                                 |
| [INLONG-9473](https://github.com/apache/inlong/issues/9473) | [Feature][Sort] Support transform of embedding                                                |
| [INLONG-9626](https://github.com/apache/inlong/issues/9626) | [Improve][Sort] Change the variable name from tid to streamId                                 |
| [INLONG-9491](https://github.com/apache/inlong/issues/9491) | [Improve][Sort] Csv format support ignore trailing unmappable fields                          |
| [INLONG-9479](https://github.com/apache/inlong/issues/9479) | [Improve][Sort] There are too many flink version definitions                                  |
| [INLONG-9616](https://github.com/apache/inlong/issues/9616) | [Bug][Sort] Failed to create pulsar producer with the same topic                              |
| [INLONG-9501](https://github.com/apache/inlong/issues/9501) | [Bug][Sort] Modify EncryptFunction                                                            |
| [INLONG-9499](https://github.com/apache/inlong/issues/9499) | [Bug][Sort] Typos in FlinkPulsarSource and  FlinkPulsarSourceWithoutAdmin                     |

### TubeMQ
|                            ISSUE                            | Summary                                                      |
|:-----------------------------------------------------------:|:-------------------------------------------------------------|
| [INLONG-9603](https://github.com/apache/inlong/issues/9603) | [Bug][TubeMQ] Cpp client build failed                        |
| [INLONG-9033](https://github.com/apache/inlong/issues/9033) | [Improve][TubeMQ] Docker container is missing tubectl script |

### Other
|                            ISSUE                            | Summary                                                                                                                        |
|:-----------------------------------------------------------:|:-------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-9541](https://github.com/apache/inlong/issues/9541) | [Feature][ASF] Remove branch protection strategy for the master branch                                                         |
| [INLONG-8391](https://github.com/apache/inlong/issues/8391) | [Improve] Optimize the HeartbeatMsg class                                                                                      |
| [INLONG-9640](https://github.com/apache/inlong/issues/9640) | [Improve][CVE] Apache Shiro vulnerable to path traversal                                                                       |
| [INLONG-9048](https://github.com/apache/inlong/issues/9048) | [Improve][CVE] Authorization Bypass Through User-Controlled Key vulnerability in Apache ZooKeeper                              |
| [INLONG-9465](https://github.com/apache/inlong/issues/9465) | [Improve][CVE] Authorization Bypass Through User-Controlled Key vulnerability in Apache ZooKeeper                              |
| [INLONG-9666](https://github.com/apache/inlong/issues/9666) | [Improve][CVE] Domain restrictions bypass via DNS Rebinding in WireMock and WireMock Studio webhooks, proxy and recorder modes |
| [INLONG-9644](https://github.com/apache/inlong/issues/9644) | [Improve][CVE] Guava vulnerable to insecure use of temporary directory                                                         |
| [INLONG-9477](https://github.com/apache/inlong/issues/9477) | [Improve][CVE] Observable Discrepancy in Apache Kafka                                                                          |
| [INLONG-9642](https://github.com/apache/inlong/issues/9642) | [Improve][CVE] TemporaryFolder on unix-like systems does not limit access to created files                                     |
| [INLONG-9463](https://github.com/apache/inlong/issues/9463) | [Improve][POM] Bump curator to 4.2.0                                                                                           |
| [INLONG-9525](https://github.com/apache/inlong/issues/9525) | [Improve][Tools] There is a typo for backup_module_dependencys.sh script                                                       |
| [INLONG-9493](https://github.com/apache/inlong/issues/9493) | [Bug] Logging error for the NativeFlinkSqlParser                                                                               |
