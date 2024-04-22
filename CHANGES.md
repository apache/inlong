
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

# Release InLong 1.12.0 - Released (as of 2024-04-21)
### Agent
| ISSUE                                                         | Summary                                                                                  |
|:--------------------------------------------------------------|:-----------------------------------------------------------------------------------------|
| [INLONG-9801](https://github.com/apache/inlong/issues/9801)   | [Umbrella][Agent] Add an agent installer module for agent installation                   |         
| [INLONG-9698](https://github.com/apache/inlong/issues/9698)   | [Improve][Agent] Optimize file collection initialization logic toIO                      |   
| [INLONG-9700](https://github.com/apache/inlong/issues/9700)   | [Improve][Agent] Optimize the message ack logic to reduce semaphocompetition.            |   
| [INLONG-9702](https://github.com/apache/inlong/issues/9702)   | [Improve][Agent] Change the data transmission interval to depenconfiguration             |
| [INLONG-9704](https://github.com/apache/inlong/issues/9704)   | [Improve][Agent] Modify the default value of memory control semapdata                    |
| [INLONG-9710](https://github.com/apache/inlong/issues/9710)   | [Improve][Agent] Improve the accuracy of instance heartbeat auditing                     |
| [INLONG-9712](https://github.com/apache/inlong/issues/9712)   | [Improve][Agent] Adjusting task configuration verification logic                         |
| [INLONG-9714](https://github.com/apache/inlong/issues/9714)   | [Improve][Agent] SQL injection in pgjdbc                                                 |
| [INLONG-9716](https://github.com/apache/inlong/issues/9716)   | [Improve][Agent] Delete useless code when storing tasks                                  |
| [INLONG-9721](https://github.com/apache/inlong/issues/9721)   | [Improve][Agent] Add a common cycle parameter to the task configuration                  |
| [INLONG-9736](https://github.com/apache/inlong/issues/9736)   | [Improve][Agent] Make time zone a common parameter                                       |
| [INLONG-9772](https://github.com/apache/inlong/issues/9772)   | [Improve][Agent] Increase auditing for sending exceptions and resending                  |
| [INLONG-9798](https://github.com/apache/inlong/issues/9798)   | [Improve][Agent] Add type for agent installer                                            |
| [INLONG-9802](https://github.com/apache/inlong/issues/9802)   | [Improve][Agent] Add an agent installer module for agent installation                    |
| [INLONG-9806](https://github.com/apache/inlong/issues/9806)   | [Improve][Agent] Add installer configuration file                                        |
| [INLONG-9816](https://github.com/apache/inlong/issues/9816)   | [Improve][Agent] Add config class for installer                                          |
| [INLONG-9829](https://github.com/apache/inlong/issues/9829)   | [Improve][Agent] Add guardian scripts                                                    |
| [INLONG-9831](https://github.com/apache/inlong/issues/9831)   | [Improve][Agent] Increase configuration acquisition capability                           |
| [INLONG-9833](https://github.com/apache/inlong/issues/9833)   | [Improve][Agent] Add module state to distinguish whether the moduinstalled               |
| [INLONG-9844](https://github.com/apache/inlong/issues/9844)   | [Improve][Agent] Add agent installer config request class                                |
| [INLONG-9848](https://github.com/apache/inlong/issues/9848)   | [Improve][Agent] Add the getAuthHeader function in the HttpManage                        |
| [INLONG-9850](https://github.com/apache/inlong/issues/9850)   | [Improve][Agent] Add a function to retrieve HttpManager in the Mo                        |
| [INLONG-9852](https://github.com/apache/inlong/issues/9852)   | [Improve][Agent] Place the configuration item for the installatioconfiguration           |
| [INLONG-9858](https://github.com/apache/inlong/issues/9858)   | [Improve][Agent] Increase local read and write capabilities formconfig                   |
| [INLONG-9859](https://github.com/apache/inlong/issues/9859)   | [Improve][Agent] Increase installation package download capabilitpower                   |
| [INLONG-9863](https://github.com/apache/inlong/issues/9863)   | [Improve][Agent] To avoid data loss caused by too many supplementfiles                   |
| [INLONG-9889](https://github.com/apache/inlong/issues/9889)   | [Improve][Agent] Delete test code                                                        |
| [INLONG-9906](https://github.com/apache/inlong/issues/9906)   | [Improve][Agent] Add configuration comparison logic and processin                        |
| [INLONG-9909](https://github.com/apache/inlong/issues/9909)   | [Improve][Agent] Add unit test for installer                                             |
| [INLONG-9910](https://github.com/apache/inlong/issues/9910)   | [Improve][Agent] Increase daily verification for process monitordownloading              |
| [INLONG-9922](https://github.com/apache/inlong/issues/9922)   | [Improve][Agent] Add a configuration copy script to copy the confagent                   |
| [INLONG-9946](https://github.com/apache/inlong/issues/9946)   | [Improve][Agent] Verify the return code        only proceed with thessuccessful          |
| [INLONG-9948](https://github.com/apache/inlong/issues/9948)   | [Improve][Agent] The instance class has many code similaritiesclass                      |
| [INLONG-9955](https://github.com/apache/inlong/issues/9955)   | [Improve][Agent] Rename Job to Task                                                      |
| [INLONG-9969](https://github.com/apache/inlong/issues/9969)   | [Improve][Agent] Release the memory semaphore of the source onlqueue                     |
| [INLONG-9982](https://github.com/apache/inlong/issues/9982)   | [Improve][Agent] Adjusting the abstraction of source code to facisources                 |
| [INLONG-9983](https://github.com/apache/inlong/issues/9983)   | [Improve][Agent] Renew the doc of adding sources                                         |
| [INLONG-9987](https://github.com/apache/inlong/issues/9987)   | [Improve][Agent] Fix the issue of deleting the first digit whent0                        |
| [INLONG-9990](https://github.com/apache/inlong/issues/9990)   | [Improve][Agent] Avoid MD5 calculation functions directly return""""                     |
| [INLONG-9997](https://github.com/apache/inlong/issues/9997)   | [Improve][Agent] Handling situations where the installation packachange                  |
| [INLONG-9999](https://github.com/apache/inlong/issues/9999)   | [Improve][Agent] Handle scenarios where the module list is emptdeletion                  |
| [INLONG-10010](https://github.com/apache/inlong/issues/10010) | [Improve][Agent] Adjust source encapsulation        keep public ininitSource             |
| [INLONG-10012](https://github.com/apache/inlong/issues/10012) | [Improve][Agent] Adjust task encapsulation to place common logiclass                     |
| [INLONG-9804](https://github.com/apache/inlong/issues/9804)   | [Feature][Agent] Add Pulsar Source for Agent                                             |
| [INLONG-10006](https://github.com/apache/inlong/issues/10006) | [Feature][Agent] Add MongoDB data source for Agent                                       |
| [INLONG-8962](https://github.com/apache/inlong/issues/8962)   | [Bug][Agent] Before state and after state are the same in StateCallback.call             |
| [INLONG-9683](https://github.com/apache/inlong/issues/9683)   | [Bug][Agent] time to remove Akka and use Apache Pekko?                                   |

### Dashboard
| ISSUE                                                         | Summary                                                                                  |
|:--------------------------------------------------------------|:-----------------------------------------------------------------------------------------|
| [INLONG-9708](https://github.com/apache/inlong/issues/9708)   | [Improve][Dashboard] Module audit query date optimization                                |
| [INLONG-9723](https://github.com/apache/inlong/issues/9723)   | [Improve][Dashboard] Module audit id query item optimization                             |
| [INLONG-9729](https://github.com/apache/inlong/issues/9729)   | [Improve][Dashboard] Data access sink field mapping supports underscores                 |  
| [INLONG-9747](https://github.com/apache/inlong/issues/9747)   | [Improve][Dashboard] Module audit ID query data display optimization                     |
| [INLONG-9777](https://github.com/apache/inlong/issues/9777)   | [Improve][Dashboard] Module audit ip query display optimization                          |
| [INLONG-9820](https://github.com/apache/inlong/issues/9820)   | [Improve][Dashboard] Update Pulsar source field                                          |
| [INLONG-9842](https://github.com/apache/inlong/issues/9842)   | [Improve][Dashboard] Data access resource details support paging                         |
| [INLONG-9895](https://github.com/apache/inlong/issues/9895)   | [Improve][Dashboard] It also needs to support restarting when thefails                   |
| [INLONG-9963](https://github.com/apache/inlong/issues/9963)   | [Improve][Dashboard] Kafka and Pulsar source is missing agent ifield                     |
| [INLONG-10008](https://github.com/apache/inlong/issues/10008) | [Improve][Dashboard][Manager] Supplement fields of MongoDB dataDashboard                 |
| [INLONG-9837](https://github.com/apache/inlong/issues/9837)   | [Bug][Dasgboard] Pulsar cluster management displays errors                               |
| [INLONG-9950](https://github.com/apache/inlong/issues/9950)   | [Bug][Dashboard] Audit query uses incorrect end time                                     |
| [INLONG-10001](https://github.com/apache/inlong/issues/10001) | [Bug][Dashboard] End date time initialization error on the audipage                      |

### Manager
| ISSUE                                                         | Summary                                                                                  |
|:--------------------------------------------------------------|:-----------------------------------------------------------------------------------------|
| [INLONG-9689](https://github.com/apache/inlong/issues/9689)   | [Improve][Manager] Optimize MySQL JDBC URL check                                         |
| [INLONG-9706](https://github.com/apache/inlong/issues/9706)   | [Improve][Manager] Supports jdbc verification under multiple hosts                       |
| [INLONG-9718](https://github.com/apache/inlong/issues/9718)   | [Improve][Manager] Transform supports function type fields                               |
| [INLONG-9727](https://github.com/apache/inlong/issues/9727)   | [Improve][Manager] Support configuring the timezone for agent colstreamSource            |
| [INLONG-9733](https://github.com/apache/inlong/issues/9733)   | [Improve][Manager] Support querying audit data at the minute level                       |
| [INLONG-9735](https://github.com/apache/inlong/issues/9735)   | [Improve][Manager] Spring Web vulnerable to Open Redirect or ServForgery                 |
| [INLONG-9752](https://github.com/apache/inlong/issues/9752)   | [Improve][Manager] Operation logs support querying based on succecalls                   |
| [INLONG-9756](https://github.com/apache/inlong/issues/9756)   | [Improve][Manager] flink job name should be more readable                                |
| [INLONG-9768](https://github.com/apache/inlong/issues/9768)   | [Improve][Manager] Optimize flink job building and manage procedure                      |
| [INLONG-9770](https://github.com/apache/inlong/issues/9770)   | [Improve][Manager] Unified compression type configuration                                |
| [INLONG-9808](https://github.com/apache/inlong/issues/9808)   | [Improve][Manager] Set the ignoreParseError field to null                                |
| [INLONG-9818](https://github.com/apache/inlong/issues/9818)   | [Improve][Manger] Decode Msg based on the manager's configuration                        |
| [INLONG-9825](https://github.com/apache/inlong/issues/9825)   | [Improve][Manager] reduce the creation of RestClusterClient                              |
| [INLONG-9839](https://github.com/apache/inlong/issues/9839)   | [Improve][Manager] Optimize the auto assign logic of SortStandalocluster                 |
| [INLONG-9846](https://github.com/apache/inlong/issues/9846)   | [Improve][Manager] Optimize slow query SQL                                               |
| [INLONG-9860](https://github.com/apache/inlong/issues/9860)   | [Improve][Manager] Manager client supports querying workflow logs                        |
| [INLONG-9873](https://github.com/apache/inlong/issues/9873)   | [Improve][Manager] Support adding data add tasks for file collection                     |
| [INLONG-9881](https://github.com/apache/inlong/issues/9881)   | [Improve][Manager] Add unit test of AuditController                                      |
| [INLONG-9883](https://github.com/apache/inlong/issues/9883)   | [Improve][Manager] Add unit test of HeartbeatController                                  |
| [INLONG-9902](https://github.com/apache/inlong/issues/9902)   | [Improve][Manager] Data preview supports pulsar multi cluster                            |
| [INLONG-9932](https://github.com/apache/inlong/issues/9932)   | [Improve][Manager] Add an agent installer module management for agent installation       |
| [INLONG-9962](https://github.com/apache/inlong/issues/9962)   | [Improve][Manager] Data preview supports returning header and specific field information |
| [INLONG-9968](https://github.com/apache/inlong/issues/9968)   | [Improve][Manager] Support pulsar multi cluster when creating pulsar consumption groups  |
| [INLONG-9974](https://github.com/apache/inlong/issues/9974)   | [Improve][Manager] Data preview simplifies interface field information                   |
| [INLONG-9976](https://github.com/apache/inlong/issues/9976)   | [Improve][Manager] Support multiple types of audit indicator queries                     |
| [INLONG-9980](https://github.com/apache/inlong/issues/9980)   | [Improve][Manager] Remove the derby.jar file from the manager project                    |
| [INLONG-9985](https://github.com/apache/inlong/issues/9985)   | [Improve][Manager] Support authentication params for pulsar source                       |
| [INLONG-9995](https://github.com/apache/inlong/issues/9995)   | [Improve][Manager] Support batch saving of group information and other operations        |
| [INLONG-9884](https://github.com/apache/inlong/issues/9884)   | [Improve][Manager] Optimized code AuditServiceImpl.java                                  |
| [INLONG-9886](https://github.com/apache/inlong/issues/9886)   | [Improve][Manager] Optimized code DataProxyConfigRepositoryV2                            |
| [INLONG-9696](https://github.com/apache/inlong/issues/9696)   | [Feature][Manager] Manager client support delete inlong tenant                           |
| [INLONG-9773](https://github.com/apache/inlong/issues/9773)   | [Feature][Manager] SortSdk configuration support  acquire tenanInlongGroup               |
| [INLONG-9781](https://github.com/apache/inlong/issues/9781)   | [Feature][Manager] Support offline synchronization task definition                       |
| [INLONG-9813](https://github.com/apache/inlong/issues/9813)   | [Feature][Manager] Support offline data sync management                                  |
| [INLONG-9822](https://github.com/apache/inlong/issues/9822)   | [Feature][Manager] Support flink job runtime execution mode configuration                |
| [INLONG-9862](https://github.com/apache/inlong/issues/9862)   | [Feature][Manager] Support submit flink job for offline data sync                        |
| [INLONG-9870](https://github.com/apache/inlong/issues/9870)   | [Feature][Manager] Pulsar DataNode support to set compression type                       |
| [INLONG-9960](https://github.com/apache/inlong/issues/9960)   | [Feature][Manager] Manager support to config Kafka data node                             |
| [INLONG-9742](https://github.com/apache/inlong/issues/9742)   | [Bug][Manager] Mysql miss field data_time_zone                                           |
| [INLONG-9760](https://github.com/apache/inlong/issues/9760)   | [Bug][Manager] resource may leak due to BufferedReader not closed                        |
| [INLONG-9793](https://github.com/apache/inlong/issues/9793)   | [Bug][Manager] Manager client workflowApi.listprocess failed topcorrectly                |
| [INLONG-9827](https://github.com/apache/inlong/issues/9827)   | [Bug][Manager] Failed to check if the consumption group exists                           |
| [INLONG-9856](https://github.com/apache/inlong/issues/9856)   | [Bug][Manager] Missing tenant information when listTag                                   |
| [INLONG-9876](https://github.com/apache/inlong/issues/9876)   | [Bug][Manager] manager client error message has wrong format                             |
| [INLONG-9917](https://github.com/apache/inlong/issues/9917)   | [Bug][Manager] Manager restart data sync job failed                                      |
| [INLONG-9921](https://github.com/apache/inlong/issues/9921)   | [Bug][Mananger]  manager can't stop data sync job                                        |
| [INLONG-9953](https://github.com/apache/inlong/issues/9953)   | [Bug][Manager] stop stream source failed                                                 |
                                                         
### SDK
| ISSUE                                                         | Summary                                                                                  |
|:--------------------------------------------------------------|:-----------------------------------------------------------------------------------------|
| [INLONG-9762](https://github.com/apache/inlong/issues/9762)   | [Bug][SDK] DataProxy SDK Connect Manager error.                                          |

### Sort
| ISSUE                                                         | Summary                                                                                                                                                                                |
|:--------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-9687](https://github.com/apache/inlong/issues/9687)   | [Improve][Sort] Remove useless configuration items in pom.xml                                                                                                                          |
| [INLONG-9739](https://github.com/apache/inlong/issues/9739)   | [Improve][Sort] onParsingBodyFailure function in FailureHandlercmetric.                                                                                                                |
| [INLONG-9795](https://github.com/apache/inlong/issues/9795)   | [Improve][Sort] Regarding the format optimization of the data typmoduleRegarding the format optimization of the data type enumeration efinition of the Redis connector project module  |
| [INLONG-9913](https://github.com/apache/inlong/issues/9913)   | [Improve][Sort] Solve end-to-end-tests-v1.13 naming problem andtfailed.                                                                                                                |
| [INLONG-8948](https://github.com/apache/inlong/issues/8948)   | [Feature][Sort]  Add Redis connector on flink 1.15                                                                                                                                     |
| [INLONG-9758](https://github.com/apache/inlong/issues/9758)   | [Feature][Sort] StarRocks connector support state key when initializing                                                                                                                |
| [INLONG-9774](https://github.com/apache/inlong/issues/9774)   | [Feature][Sort] Support rowdata way of sort InLong message tlog-kv format                                                                                                              |
| [INLONG-9788](https://github.com/apache/inlong/issues/9788)   | [Feature][Sort] Supports data parse that contains delimiters in kv and csv data content                                                                                                |
| [INLONG-9835](https://github.com/apache/inlong/issues/9835)   | [Feature][Sort] Add Redis connector on flink 1.15                                                                                                                                      |
| [INLONG-9871](https://github.com/apache/inlong/issues/9871)   | [Feature][Sort] Use ZLIB as the default compression type of pulsar sink                                                                                                                |
| [INLONG-9972](https://github.com/apache/inlong/issues/9972)   | [Feature][Sort] Pulsar connetor should support authentication when connecting to Pulsar cluster                                                                                        |
| [INLONG-9899](https://github.com/apache/inlong/issues/9899)   | [Feature][Sort] Pulsar Extract Node supports authentication                                                                                                                            |
| [INLONG-10017](https://github.com/apache/inlong/issues/10017) | [Feature][Sort] Definition of Sort Config                                                                                                                                              |
| [INLONG-9753](https://github.com/apache/inlong/issues/9753)   | [Bug][Sort] module inlong-sort-standalone's  conf/sid_es_v3.conlicense                                                                                                                 |
| [INLONG-9854](https://github.com/apache/inlong/issues/9854)   | [Bug][Sort] rowdata-tlogkv module artifactId is error, need change                                                                                                                     |
| [INLONG-9875](https://github.com/apache/inlong/issues/9875)   | [Bug][Sort] DorisLoadNode repeat                                                                                                                                                       |
| [INLONG-9879](https://github.com/apache/inlong/issues/9879)   | [Bug][Sort] Sort inlongmsg-rowdata-kv format parse head has error                                                                                                                      |
| [INLONG-9925](https://github.com/apache/inlong/issues/9925)   | [Bug][Sort] Infinite log growth causes insufficient disk space onClickhouse)                                                                                                           |
| [INLONG-9965](https://github.com/apache/inlong/issues/9965)   | [Bug][Sort] Wrong node duration of SortStandalone Pulsar Sink                                                                                                                          |
| [INLONG-9695](https://github.com/apache/inlong/issues/9695)   | [Bug][Sort] Fix kafka extract node option config building errorwconnector                                                                                                              |

### Audit
| ISSUE                                                         | Summary                                                                             |
|:--------------------------------------------------------------|:------------------------------------------------------------------------------------|
| [INLONG-9807](https://github.com/apache/inlong/issues/9807)   | [Improve][Audit] Add debug log for audit-proxy                                      |
| [INLONG-10014](https://github.com/apache/inlong/issues/10014) | [Improve][Audit] Add new audit SDK API for Flink Sort                               |
| [INLONG-9766](https://github.com/apache/inlong/issues/9766)   | [Feature][Audit] Support user-defined SocketAddress loader gettinAuditProxy         |
| [INLONG-9797](https://github.com/apache/inlong/issues/9797)   | [Feature][Audit] Audit-sdk reporting supports version number dimension              |
| [INLONG-9811](https://github.com/apache/inlong/issues/9811)   | [Feature][Audit] SDK supports both singleton and non-singleton usage                |
| [INLONG-9904](https://github.com/apache/inlong/issues/9904)   | [Feature][Audit] SDK supports checkpoint feature                                    |
| [INLONG-9907](https://github.com/apache/inlong/issues/9907)   | [Feature][Audit] Audit-service add codes of entities                                |
| [INLONG-9914](https://github.com/apache/inlong/issues/9914)   | [Feature][Audit] Add SQL related to audit-service                                   |
| [INLONG-9920](https://github.com/apache/inlong/issues/9920)   | [Feature][Audit] Audit-service add codes of source                                  |
| [INLONG-9926](https://github.com/apache/inlong/issues/9926)   | [Feature][Audit] Audit-service support HA active and backup                         |
| [INLONG-9928](https://github.com/apache/inlong/issues/9928)   | [Feature][Audit] Audit-service HA election through mysql                            |
| [INLONG-9957](https://github.com/apache/inlong/issues/9957)   | [Feature][Audit] Audit-service add local cache for openapi                          |
| [INLONG-9977](https://github.com/apache/inlong/issues/9977)   | [Feature][Audit] Audit-service increases the capabilities of openapi                |
| [INLONG-9989](https://github.com/apache/inlong/issues/9989)   | [Feature][Audit] Audit-store support the feature of audit-version                   |
| [INLONG-10003](https://github.com/apache/inlong/issues/10003) | [Feature][Audit] Audit-service supports multiple data source clusters               |

### Other
| ISSUE                                                         | Summary                                                                             |
|:--------------------------------------------------------------|:------------------------------------------------------------------------------------|
| [INLONG-9934](https://github.com/apache/inlong/issues/9934)   | [Improve][doc] update copyright date to 2024                                        |
| [INLONG-9893](https://github.com/apache/inlong/issues/9893)   | [Improve][Common] Optimized code InLongMsg.java                                     |
| [INLONG-9897](https://github.com/apache/inlong/issues/9897)   | [Improve][Common] Code cognitive complexity modification                            |
| [INLONG-9690](https://github.com/apache/inlong/issues/9690)   | [Bug][CI] No space left on device for build                                         |