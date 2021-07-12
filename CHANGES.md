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

## Release 0.9.0-incubating - Released (as of 2021-07-11)

### IMPROVEMENTS:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-594](https://issues.apache.org/jira/browse/INLONG-594) | Trpc-go  tube sdk strongly rely on local config  | Major |
| [INLONG-616](https://issues.apache.org/jira/browse/INLONG-616) | Adjust the content in .asf.yaml according to the new project name  | Major |
| [INLONG-651](https://issues.apache.org/jira/browse/INLONG-651) | Refine the tubemq-client-cpp build description  | Major |
| [INLONG-655](https://issues.apache.org/jira/browse/INLONG-655) | add dependency in tube-manager  | Major |
| [INLONG-656](https://issues.apache.org/jira/browse/INLONG-656) | fix tubemanager start.sh  | Major |
| [INLONG-657](https://issues.apache.org/jira/browse/INLONG-657) | remove some test cases in agent  | Major |
| [INLONG-659](https://issues.apache.org/jira/browse/INLONG-659) | fix unit test in agent  | Major |
| [INLONG-666](https://issues.apache.org/jira/browse/INLONG-666) | change tar name in agent  | Major |
| [INLONG-697](https://issues.apache.org/jira/browse/INLONG-697) | fix decode error in proxySdk  | Major |
| [INLONG-705](https://issues.apache.org/jira/browse/INLONG-705) | add stop.sh in dataproxy  | Major |

### BUG FIXES:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-577](https://issues.apache.org/jira/browse/INLONG-577) | reload status output and topic config output mismatch | Major |
| [INLONG-612](https://issues.apache.org/jira/browse/INLONG-612) | Restful api "admin_snapshot_message" is not compatible with the old version | Major |
| [INLONG-638](https://issues.apache.org/jira/browse/INLONG-638) | Issues About Disk Error recovery | Major |
| [INLONG-688](https://issues.apache.org/jira/browse/INLONG-688) | change additionstr to additionAtr in agent | Major |
| [INLONG-703](https://issues.apache.org/jira/browse/INLONG-703) | Query after adding a consumer group policy and report a null error | Major |
| [INLONG-724](https://issues.apache.org/jira/browse/INLONG-724) | Encountered a symbol not found error when compiling | Major |

### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [INLONG-613](https://issues.apache.org/jira/browse/INLONG-613) | Adjust the project codes according to the renaming requirements  | Major |

### SUB-TASK:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-519](https://issues.apache.org/jira/browse/INLONG-519) | Bumped version to 0.9.0-SNAPSHOT | Major |
| [INLONG-565](https://issues.apache.org/jira/browse/INLONG-565) | Replace simple scripts and code implementation | Major |
| [INLONG-571](https://issues.apache.org/jira/browse/INLONG-571) | Adjust WebOtherInfoHandler class implementation | Major |
| [INLONG-573](https://issues.apache.org/jira/browse/INLONG-573) | Adjust WebAdminFlowRuleHandler class implementation | Major |
| [INLONG-574](https://issues.apache.org/jira/browse/INLONG-574) | Adjust WebAdminGroupCtrlHandler class implementation | Major |
| [INLONG-632](https://issues.apache.org/jira/browse/INLONG-632) | Add inlong-manager subdirectory | Major |
| [INLONG-633](https://issues.apache.org/jira/browse/INLONG-633) | Add inlong-sort subdirectory | Major |
| [INLONG-634](https://issues.apache.org/jira/browse/INLONG-634) | Add inlong-tubemq subdirectory | Major |
| [INLONG-635](https://issues.apache.org/jira/browse/INLONG-635) | Add inlong-dataproxy subdirectory | Major |
| [INLONG-636](https://issues.apache.org/jira/browse/INLONG-636) | Add inlong-agent subdirectory | Major |
| [INLONG-640](https://issues.apache.org/jira/browse/INLONG-640) | Adjust the main frame of the InLong project | Major |
| [INLONG-641](https://issues.apache.org/jira/browse/INLONG-641) | Add inlong-common module | Major |
| [INLONG-642](https://issues.apache.org/jira/browse/INLONG-642) | Add inlong-website subdirectory | Major |
| [INLONG-643](https://issues.apache.org/jira/browse/INLONG-643) | Add inlong-dataproxy-sdk subdirectory | Major |
| [INLONG-644](https://issues.apache.org/jira/browse/INLONG-644) | Remove "/dist" from .gitignore and add subdirectory dist in inlong-sort | Major |
| [INLONG-646](https://issues.apache.org/jira/browse/INLONG-646) | Remove time related unit tests until timezone is configurable in inlong-sort | Major |
| [INLONG-647](https://issues.apache.org/jira/browse/INLONG-647) | Adjust the introduction content of the README.md | Major |
| [INLONG-648](https://issues.apache.org/jira/browse/INLONG-648) | Change initial version of inlong-sort to 0.9.0-incubating-SNAPSHOT | Major |
| [INLONG-649](https://issues.apache.org/jira/browse/INLONG-649) | Modify the suffix of the docs/zh-cn/download/release-0.8.0.md file | Major |
| [INLONG-650](https://issues.apache.org/jira/browse/INLONG-650) | Adjust .asf.yaml's label | Major |
| [INLONG-654](https://issues.apache.org/jira/browse/INLONG-654) | Adjust the link in ReadMe.md according to the latest document | Major |
| [INLONG-658](https://issues.apache.org/jira/browse/INLONG-658) | modify the dependency of inlong-manager | Major |
| [INLONG-663](https://issues.apache.org/jira/browse/INLONG-663) | modify the tar package name of inlong-manager | Major |
| [INLONG-667](https://issues.apache.org/jira/browse/INLONG-667) | rename tar in agent | Major |
| [INLONG-673](https://issues.apache.org/jira/browse/INLONG-673) | add tube cluster id in inlong-manager | Major |
| [INLONG-674](https://issues.apache.org/jira/browse/INLONG-674) | adjust HiveSinkInfo in inlong-manager | Major |
| [INLONG-675](https://issues.apache.org/jira/browse/INLONG-675) | modify the npm script of inlong-website | Major |
| [INLONG-676](https://issues.apache.org/jira/browse/INLONG-676) | modify manager-web to manager-api in inlong-manager module | Major |
| [INLONG-677](https://issues.apache.org/jira/browse/INLONG-677) | Support dt as built-in data time partition field in inlong-sort | Major |
| [INLONG-681](https://issues.apache.org/jira/browse/INLONG-681) | modify assembly in agent proxy tubemanager | Major |
| [INLONG-687](https://issues.apache.org/jira/browse/INLONG-687) | set schemaName to m0_day when save business in inlong-manager | Major |
| [INLONG-689](https://issues.apache.org/jira/browse/INLONG-689) | add sort app name | Major |
| [INLONG-691](https://issues.apache.org/jira/browse/INLONG-691) | update properties and scripts for inlong-manager | Major |
| [INLONG-692](https://issues.apache.org/jira/browse/INLONG-692) | remove hive cluster entity in inlong-manager | Major |
| [INLONG-693](https://issues.apache.org/jira/browse/INLONG-693) | change data type to tdmsg | Major |
| [INLONG-694](https://issues.apache.org/jira/browse/INLONG-694) | Add retry mechanism for creating tube consumer group | Major |
| [INLONG-695](https://issues.apache.org/jira/browse/INLONG-695) | update getConfig API in inlong-manager | Major |
| [INLONG-696](https://issues.apache.org/jira/browse/INLONG-696) | modify the status of the entities after approval | Major |
| [INLONG-699](https://issues.apache.org/jira/browse/INLONG-699) | Fix serialization issue of DeserializationInfo 's subType in inlong-sort | Major |
| [INLONG-700](https://issues.apache.org/jira/browse/INLONG-700) | Optimize dependencies in inlong-sort | Major |
| [INLONG-701](https://issues.apache.org/jira/browse/INLONG-701) | Update create resource workflow definition | Major |
| [INLONG-702](https://issues.apache.org/jira/browse/INLONG-702) | sort config field spillter | Major |
| [INLONG-706](https://issues.apache.org/jira/browse/INLONG-706) | Add 0.9.0 version release modification to CHANGES.md | Major |
| [INLONG-709](https://issues.apache.org/jira/browse/INLONG-709) | Adjust the version information of all pom.xml to 0.9.0-incubating | Major |
| [INLONG-712](https://issues.apache.org/jira/browse/INLONG-712) | adjust partition info for hive sink | Major |
| [INLONG-713](https://issues.apache.org/jira/browse/INLONG-713) | add partition filed when create hive table | Major |
| [INLONG-714](https://issues.apache.org/jira/browse/INLONG-714) | Enable checkpointing in inlong-sort | Major |
| [INLONG-715](https://issues.apache.org/jira/browse/INLONG-715) | Make shouldRollOnCheckpoint always return true in DefaultRollingPolicy in inlong-sort | Major |
| [INLONG-716](https://issues.apache.org/jira/browse/INLONG-716) | set terminated symbol when create hive table | Major |
| [INLONG-717](https://issues.apache.org/jira/browse/INLONG-717) | Declare the 3rd party Catagory x LICENSE components in use | Major |


## Release 0.8.0-incubating - Released (as of 2021-01-18)

### IMPROVEMENTS:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-430](https://issues.apache.org/jira/browse/INLONG-430) | Optimizing the implementation of HTTP API for broke  | Major |
| [INLONG-445](https://issues.apache.org/jira/browse/INLONG-445) | Adjust the status check default sleep interval of pullConsumeReadyChkSliceMs  | Major |
| [INLONG-448](https://issues.apache.org/jira/browse/INLONG-448) | Add Committer and PPMC operation process  | Major |
| [INLONG-449](https://issues.apache.org/jira/browse/INLONG-449) | Adjust Example implementation  | Major |
| [INLONG-452](https://issues.apache.org/jira/browse/INLONG-452) | Optimize rebalance performance | Major |
| [INLONG-467](https://issues.apache.org/jira/browse/INLONG-467) | Add WEB APIs of Master and Broker | Major |
| [INLONG-489](https://issues.apache.org/jira/browse/INLONG-489) | Add the maximum message length parameter setting | Major |
| [INLONG-495](https://issues.apache.org/jira/browse/INLONG-495) | Code implementation adjustment based on SpotBugs check | Major |
| [INLONG-511](https://issues.apache.org/jira/browse/INLONG-511) | Replace the conditional operator (?:) with mid()  | Major |
| [INLONG-512](https://issues.apache.org/jira/browse/INLONG-512) | Add package length control based on Topic  | Major |
| [INLONG-515](https://issues.apache.org/jira/browse/INLONG-515) | Add cluster Topic view web api  | Major |

### BUG FIXES:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-437](https://issues.apache.org/jira/browse/INLONG-437) | Fix tubemq table source sink factory instance creating problem | Major |
| [INLONG-441](https://issues.apache.org/jira/browse/INLONG-441) | An error occurred when using the Tubemq class to create a sink table | Major |
| [INLONG-442](https://issues.apache.org/jira/browse/INLONG-442) | Modifying the jvm parameters when the broker starts does not take effect  | Major    |
| [INLONG-443](https://issues.apache.org/jira/browse/INLONG-443) | TubemqSourceFunction class prints too many logs problem | Major |
| [INLONG-446](https://issues.apache.org/jira/browse/INLONG-446) | Small bugs fix that do not affect the main logics | Major |
| [INLONG-450](https://issues.apache.org/jira/browse/INLONG-450) | TubeClientException: Generate producer id failed  | Major |
| [INLONG-453](https://issues.apache.org/jira/browse/INLONG-453) | TubemqSourceFunction class prints too many logs problem | Major |
| [INLONG-506](https://issues.apache.org/jira/browse/INLONG-506) | cmakelist error | Major |
| [INLONG-510](https://issues.apache.org/jira/browse/INLONG-510) | Found a bug in MessageProducerExample class | Major |
| [INLONG-518](https://issues.apache.org/jira/browse/INLONG-518) | fix parameter pass error | Major |
| [INLONG-526](https://issues.apache.org/jira/browse/INLONG-526) | Adjust the packaging script and version check list, remove the "-WIP" tag | Major |
| [INLONG-555](https://issues.apache.org/jira/browse/INLONG-555) | short session data can only be written to a specific partition | Major |
| [INLONG-556](https://issues.apache.org/jira/browse/INLONG-556) | Index value is bigger than the actual number of records | Low |


### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [INLONG-505](https://issues.apache.org/jira/browse/INLONG-505) | Remove the "WIP" label of the DISCLAIMER file  | Major |
| [INLONG-543](https://issues.apache.org/jira/browse/INLONG-543) | Modify the LICENSE statement of multiple files and others  | Major |
| [INLONG-557](https://issues.apache.org/jira/browse/INLONG-557) | Handle the issues mentioned in the 0.8.0-RC2 review  | Major |
| [INLONG-562](https://issues.apache.org/jira/browse/INLONG-562) | Update project contents according to the 0.8.0-RC3 review  | Major |

### SUB-TASK:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-428](https://issues.apache.org/jira/browse/INLONG-433) | Bumped version to 0.8.0-SNAPSHOT | Major |
| [INLONG-433](https://issues.apache.org/jira/browse/INLONG-433) | add tubemq perf-consumer/producer scripts | Major |
| [INLONG-434](https://issues.apache.org/jira/browse/INLONG-434) | Adjust Broker API mapping | Major |
| [INLONG-435](https://issues.apache.org/jira/browse/INLONG-435) | Create Web field Mapping | Major |
| [INLONG-436](https://issues.apache.org/jira/browse/INLONG-436) | Adjust Broker's HTTP API implementation | Major |
| [INLONG-439](https://issues.apache.org/jira/browse/INLONG-439) | Add Cli field Scheme definition | Major |
| [INLONG-440](https://issues.apache.org/jira/browse/INLONG-440) | Add feature package tube-manager to zip  | Major |
| [INLONG-444](https://issues.apache.org/jira/browse/INLONG-444) | Add consume and produce Cli commands | Major |
| [INLONG-447](https://issues.apache.org/jira/browse/INLONG-447) | Add Broker-Admin Cli | Major |
| [INLONG-451](https://issues.apache.org/jira/browse/INLONG-451) | Replace ConsumeTupleInfo with Tuple2  | Major    |
| [INLONG-457](https://issues.apache.org/jira/browse/INLONG-457) | There is no need to return StringBuilder in Master.java | Major |
| [INLONG-463](https://issues.apache.org/jira/browse/INLONG-463) | Adjust Master rebalance process implementation  | Major |
| [INLONG-464](https://issues.apache.org/jira/browse/INLONG-464) | Add parameter rebalanceParallel in master.ini | Major |
| [INLONG-470](https://issues.apache.org/jira/browse/INLONG-470) | Add query API of TopicName and BrokerId collection  | Major |
| [INLONG-471](https://issues.apache.org/jira/browse/INLONG-471) | Add query API Introduction of TopicName and BrokerId collection | Major |
| [INLONG-472](https://issues.apache.org/jira/browse/INLONG-472) | Adjust Broker's AbstractWebHandler class implementation  | Major |
| [INLONG-475](https://issues.apache.org/jira/browse/INLONG-475) | add the offset clone api of the consume group  | Major |
| [INLONG-482](https://issues.apache.org/jira/browse/INLONG-482) | Add offset query api  | Major |
| [INLONG-484](https://issues.apache.org/jira/browse/INLONG-484) | Add query API for topic publication information  | Major |
| [INLONG-485](https://issues.apache.org/jira/browse/INLONG-485) | Add the batch setting API of consume group offset  | Major |
| [INLONG-486](https://issues.apache.org/jira/browse/INLONG-486) | Add the delete API of consumer group offset  | Major |
| [INLONG-494](https://issues.apache.org/jira/browse/INLONG-494) | Update API interface instruction document | Major |
| [INLONG-499](https://issues.apache.org/jira/browse/INLONG-499) | Add configure store  | Major |
| [INLONG-500](https://issues.apache.org/jira/browse/INLONG-500) | Add setting operate API  | Major |
| [INLONG-501](https://issues.apache.org/jira/browse/INLONG-501) | Adjust max message size check logic  | Major |
| [INLONG-502](https://issues.apache.org/jira/browse/INLONG-502) | Add setting API interface document  | Major |
| [INLONG-504](https://issues.apache.org/jira/browse/INLONG-504) | Adjust the WebMethodMapper class interfaces  | Major |
| [INLONG-508](https://issues.apache.org/jira/browse/INLONG-508) | Optimize Broker's PB parameter check processing logic  | Major |
| [INLONG-509](https://issues.apache.org/jira/browse/INLONG-509) | Adjust the packet length check when data is loaded  | Major |
| [INLONG-522](https://issues.apache.org/jira/browse/INLONG-522) | Add admin_query_cluster_topic_view API document  | Major |
| [INLONG-544](https://issues.apache.org/jira/browse/INLONG-544) | Adjust the LICENSE statement in the client.conf files of Python and C/C++ SDK | Major |
| [INLONG-546](https://issues.apache.org/jira/browse/INLONG-546) | Restore the original license header of the referenced external source files | Major |
| [INLONG-547](https://issues.apache.org/jira/browse/INLONG-547) | Recode the implementation of the *Startup.java classes in the Tool package | Major |
| [INLONG-548](https://issues.apache.org/jira/browse/INLONG-548) | Handle the LICENSE authorization of font files in the resources | Major |
| [INLONG-549](https://issues.apache.org/jira/browse/INLONG-549) | Handling the problem of compilation failure | Major |
| [INLONG-550](https://issues.apache.org/jira/browse/INLONG-550) | Adjust LICENSE file content | Major |
| [INLONG-551](https://issues.apache.org/jira/browse/INLONG-551) | Adjust NOTICE file content | Major |
| [INLONG-558](https://issues.apache.org/jira/browse/INLONG-558) | Adjust the LICENSE of the file header | Major |
| [INLONG-559](https://issues.apache.org/jira/browse/INLONG-559) | Update the LICENSE file according to the 0.8.0-RC2 review | Major |
| [INLONG-560](https://issues.apache.org/jira/browse/INLONG-560) | Remove unprepared modules | Major |


## Release 0.7.0-incubating - Released (as of 2020-11-25)

### New Features:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-162](https://issues.apache.org/jira/browse/INLONG-162) | Python SDK support in TubeMQ | High |
| [INLONG-336](https://issues.apache.org/jira/browse/INLONG-336) | Propose web portal to manage tube cluster Phase-I | Major |
| [INLONG-390](https://issues.apache.org/jira/browse/INLONG-390)   | support build C++ SDK with docker image | Normal |

### IMPROVEMENTS:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-369](https://issues.apache.org/jira/browse/INLONG-369) | hope to add an option in the compilation script (like `make lib` etc...)                 | Major    |
| [INLONG-373](https://issues.apache.org/jira/browse/INLONG-373) | Reduce the redundant code of Utils::Split functions             | Major    |
| [INLONG-374](https://issues.apache.org/jira/browse/INLONG-374) | Adjust some coding style issues     | Major    |
| [INLONG-375](https://issues.apache.org/jira/browse/INLONG-375) | Add a section to the README file about how to compile the project| Major    |
| [INLONG-385](https://issues.apache.org/jira/browse/INLONG-385) | update docker images     | Major    |
| [INLONG-393](https://issues.apache.org/jira/browse/INLONG-393) | Optimize the mapping code of WEB API     | Major    |
| [INLONG-406](https://issues.apache.org/jira/browse/INLONG-406) | test_consumer.py works for both Python 2 and 3   | Minor |
| [INLONG-410](https://issues.apache.org/jira/browse/INLONG-410) | install python package and simplify test_consumer.py    | Major    |
| [INLONG-416](https://issues.apache.org/jira/browse/INLONG-416) | support consume from specified position   | Major    |
| [INLONG-417](https://issues.apache.org/jira/browse/INLONG-417) | C++ Client support parse message from binary data for Python SDK    | Major    |
| [INLONG-419](https://issues.apache.org/jira/browse/INLONG-419) | SetMaxPartCheckPeriodMs() negative number, getMessage() still  | Major    |

### BUG FIXES:

| JIRA                                                         | Summary                                                      | Priority |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :------- |
| [INLONG-365](https://issues.apache.org/jira/browse/INLONG-365) | Whether the consumption setting is wrong after the processRequest exception | Major    |
| [INLONG-370](https://issues.apache.org/jira/browse/INLONG-370) | Calling GetCurConsumedInfo API always returns failure      | Major    |
| [INLONG-376](https://issues.apache.org/jira/browse/INLONG-376) | Move pullrequests_status notifications commits mail list | Major    |
| [INLONG-366](https://issues.apache.org/jira/browse/INLONG-366) | Found a nullpointerexception bug in broker | Normal |
| [INLONG-379](https://issues.apache.org/jira/browse/INLONG-379) | Modify the memory cache size default to 3M | Normal |
| [INLONG-380](https://issues.apache.org/jira/browse/INLONG-380) | Cpp client link error when gcc optimization is disabled   | Major    |
| [INLONG-405](https://issues.apache.org/jira/browse/INLONG-405) | python sdk install files lack of the whole cpp configuration | Major |
| [INLONG-401](https://issues.apache.org/jira/browse/INLONG-401) | python sdk readme bug | Minor |
| [INLONG-407](https://issues.apache.org/jira/browse/INLONG-407) | Fix some content in README.md | Trivial |
| [INLONG-418](https://issues.apache.org/jira/browse/INLONG-418) | C++ SDK function SetMaxPartCheckPeriodMs() can't work | Major |

### SUB-TASK:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-276](https://issues.apache.org/jira/browse/INLONG-276) | add python client encode/decode protobuf message for TubeMQ RPC Protocol                                 | Major    |
| [INLONG-338](https://issues.apache.org/jira/browse/INLONG-338) | web pages for tubemq manager                                     | Major    |
| [INLONG-341](https://issues.apache.org/jira/browse/INLONG-341) | open independent sub-project for tubemq                                | Major    |
| [INLONG-342](https://issues.apache.org/jira/browse/INLONG-342) | abstract backend threads for routine management                              | Major    |
| [INLONG-346](https://issues.apache.org/jira/browse/INLONG-346) | remove chinese comments                                          | Minor |
| [INLONG-355](https://issues.apache.org/jira/browse/INLONG-355) | Add business entity for topic manager                            | Major    |
| [INLONG-361](https://issues.apache.org/jira/browse/INLONG-361) | create topic when getting request             | Major    |
| [INLONG-364](https://issues.apache.org/jira/browse/INLONG-364) | uniform response format for exception state                              | Major    |
| [INLONG-383](https://issues.apache.org/jira/browse/INLONG-383) | document about Installation/API Reference/Example                                   | Major    |
| [INLONG-387](https://issues.apache.org/jira/browse/INLONG-387) | add manager web pages                                       | Major    |
| [INLONG-392](https://issues.apache.org/jira/browse/INLONG-392) | add query rest api for clusters| Major    |
| [INLONG-394](https://issues.apache.org/jira/browse/INLONG-394) | Creating Mapper class from web api to inner handler | Major    |
| [INLONG-395](https://issues.apache.org/jira/browse/INLONG-395) | Create Abstract WebHandler class                            | Major    |
| [INLONG-396](https://issues.apache.org/jira/browse/INLONG-396) | Adjust the WebXXXHandler classes implementation  | Major    |
| [INLONG-397](https://issues.apache.org/jira/browse/INLONG-397) | Add master info and other info web handler   | Major    |
| [INLONG-398](https://issues.apache.org/jira/browse/INLONG-398) | reinit project for using pybind11                            | Major    |
| [INLONG-399](https://issues.apache.org/jira/browse/INLONG-399) | expose C++ SDK method by Pybind11                                         | Major    |
| [INLONG-400](https://issues.apache.org/jira/browse/INLONG-400) | add example for consume message by bypind11                                 | Major    |
| [INLONG-402](https://issues.apache.org/jira/browse/INLONG-402) | add modify rest api for clusters                           | Major    |
| [INLONG-412](https://issues.apache.org/jira/browse/INLONG-402) | tube manager start stop scrrpts                           | Major    |
| [INLONG-415](https://issues.apache.org/jira/browse/INLONG-415) | exclude apache license for front end code  | Major    |

## Release 0.6.0-incubating - Released (as of 2020-09-25)

### New Features:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-319](https://issues.apache.org/jira/browse/INLONG-319) | In the pull mode, consumers support the  suspension of consumption for a certain partition | Major    |
| [INLONG-3](https://issues.apache.org/jira/browse/INLONG-3)   | C++ SDK support in TubeMQ                                    | Normal   |

### IMPROVEMENTS:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-311](https://issues.apache.org/jira/browse/INLONG-311) | Feedback more production information                 | Major    |
| [INLONG-312](https://issues.apache.org/jira/browse/INLONG-312) | Feedback more consumption information                | Major    |
| [INLONG-325](https://issues.apache.org/jira/browse/INLONG-325) | Add 406 ~ 408 error code to pullSelect call          | Major    |
| [INLONG-345](https://issues.apache.org/jira/browse/INLONG-345) | Optimize the call logic of getMessage() in Pull mode | Major    |
| [INLONG-352](https://issues.apache.org/jira/browse/INLONG-352) | Set the parameters of the example at startup         | Major    |
| [INLONG-353](https://issues.apache.org/jira/browse/INLONG-353) | Update LICENSE about C/C++ SDK's code reference      | Major    |
| [INLONG-356](https://issues.apache.org/jira/browse/INLONG-356) | C++ SDK Codec decode add requestid                   | Major    |
| [INLONG-327](https://issues.apache.org/jira/browse/INLONG-327) | Fix the concurrency problem in the example           | Normal   |

### BUG FIXES:

| JIRA                                                         | Summary                                                      | Priority |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :------- |
| [INLONG-316](https://issues.apache.org/jira/browse/INLONG-316) | Where the port the port is aleady used, the  process throw the exception, but not exit | Major    |
| [INLONG-317](https://issues.apache.org/jira/browse/INLONG-317) | The Store Manager throws java.lang.NullPointerException      | Major    |
| [INLONG-320](https://issues.apache.org/jira/browse/INLONG-320) | Request for static web contents would get responses with no content | Major    |
| [INLONG-354](https://issues.apache.org/jira/browse/INLONG-354) | Found a dns translate bug in C/C++ sdk                       | Major    |
| [INLONG-306](https://issues.apache.org/jira/browse/INLONG-306) | Raise Nullpointer Exception when create tubemq instance      | Low      |
| [INLONG-359](https://issues.apache.org/jira/browse/INLONG-359) | TubeMQ consume speed dropped to 0 in some partitions, it is a very serious bug | Blocker  |

### SUB-TASK:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [INLONG-250](https://issues.apache.org/jira/browse/INLONG-250) | Create C/C++ configure files                                 | Major    |
| [INLONG-251](https://issues.apache.org/jira/browse/INLONG-251) | Create C/C++ Codec utils                                     | Major    |
| [INLONG-252](https://issues.apache.org/jira/browse/INLONG-252) | Create C/C++ Metadata classes                                | Major    |
| [INLONG-262](https://issues.apache.org/jira/browse/INLONG-262) | Create C++ flow control handler                              | Major    |
| [INLONG-263](https://issues.apache.org/jira/browse/INLONG-263) | Create C/C++ ini file read utils                             | Major    |
| [INLONG-266](https://issues.apache.org/jira/browse/INLONG-266) | [INLONG-266] Add Tencent/rapidjson as submodule              | Major    |
| [INLONG-267](https://issues.apache.org/jira/browse/INLONG-267) | Create C/C++ Message class                                   | Major    |
| [INLONG-269](https://issues.apache.org/jira/browse/INLONG-269) | Create C/C++ RmtDataCache class                              | Major    |
| [INLONG-272](https://issues.apache.org/jira/browse/INLONG-272) | Unified C/C++ files's code style                             | Major    |
| [INLONG-274](https://issues.apache.org/jira/browse/INLONG-274) | Support CMake compilation                                    | Major    |
| [INLONG-275](https://issues.apache.org/jira/browse/INLONG-275) | Thread Pool & Timer                                          | Major    |
| [INLONG-280](https://issues.apache.org/jira/browse/INLONG-280) | Create C/C++ subscribe info class                            | Major    |
| [INLONG-281](https://issues.apache.org/jira/browse/INLONG-281) | atomic_def.h use C++11 stdlib class                          | Major    |
| [INLONG-282](https://issues.apache.org/jira/browse/INLONG-282) | Create C/C++ return result class                             | Major    |
| [INLONG-283](https://issues.apache.org/jira/browse/INLONG-283) | Adjust C/C++ some file names: add "tubemq_" prefix           | Major    |
| [INLONG-285](https://issues.apache.org/jira/browse/INLONG-285) | Replace C/C++ pthread's mutex to std::mutex                  | Major    |
| [INLONG-286](https://issues.apache.org/jira/browse/INLONG-286) | Create C/C++ SDK's manager class                             | Major    |
| [INLONG-287](https://issues.apache.org/jira/browse/INLONG-287) | C++ SDK io buffer                                            | Major    |
| [INLONG-288](https://issues.apache.org/jira/browse/INLONG-288) | C++ SDK Codec interface                                      | Major    |
| [INLONG-289](https://issues.apache.org/jira/browse/INLONG-289) | C++ SDK Codec TubeMQ proto support                           | Major    |
| [INLONG-290](https://issues.apache.org/jira/browse/INLONG-290) | C++ SDK TCP Connect                                          | Major    |
| [INLONG-291](https://issues.apache.org/jira/browse/INLONG-291) | C++ SDK Connect Pool                                         | Major    |
| [INLONG-293](https://issues.apache.org/jira/browse/INLONG-293) | C++ SDK Create Future class                                  | Major    |
| [INLONG-296](https://issues.apache.org/jira/browse/INLONG-296) | Adjust the version information of all pom.xml                | Major    |
| [INLONG-300](https://issues.apache.org/jira/browse/INLONG-300) | Update LICENSE                                               | Major    |
| [INLONG-308](https://issues.apache.org/jira/browse/INLONG-308) | Upgrade Jetty 6 (mortbay) => Jetty 9 (eclipse)               | Major    |
| [INLONG-309](https://issues.apache.org/jira/browse/INLONG-309) | Add POST support to WebAPI                                   | Major    |
| [INLONG-326](https://issues.apache.org/jira/browse/INLONG-326) | [website] Added 405 ~ 408 error code definition              | Major    |
| [INLONG-347](https://issues.apache.org/jira/browse/INLONG-347) | C++ SDK Create client API                                    | Major    |
| [INLONG-348](https://issues.apache.org/jira/browse/INLONG-348) | C++SDK Client handler detail                                 | Major    |
| [INLONG-349](https://issues.apache.org/jira/browse/INLONG-349) | C++ SDK Create Thread Pool                                   | Major    |
| [INLONG-350](https://issues.apache.org/jira/browse/INLONG-350) | C++ SDK client code adj                                      | Major    |
| [INLONG-351](https://issues.apache.org/jira/browse/INLONG-351) | C++ SDK example&tests                                        | Major    |
| [INLONG-358](https://issues.apache.org/jira/browse/INLONG-358) | Adjust tubemq-manager, remove it from master, and develop with INLONG-336  branch | Major    |
| [INLONG-268](https://issues.apache.org/jira/browse/INLONG-268) | C++ SDK log module                                           | Normal   |
| [INLONG-292](https://issues.apache.org/jira/browse/INLONG-292) | C++ SDK singleton & executor_pool optimization               | Normal   |
| [INLONG-270](https://issues.apache.org/jira/browse/INLONG-270) | this point c++ SDK class                                     | Minor    |
| [INLONG-271](https://issues.apache.org/jira/browse/INLONG-271) | C++ SDK copy constructor and  assignment constructor         | Minor    |
| [INLONG-273](https://issues.apache.org/jira/browse/INLONG-273) | C++ SDK dir name change inc -> include/tubemq/               | Minor    |

## Release 0.5.0-incubating - released (as of 2020-07-22)

### NEW FEATURES:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[INLONG-122](https://issues.apache.org/jira/browse/INLONG-122) | Increase JAVA version collection of SDK environment |  Major|
|[INLONG-163](https://issues.apache.org/jira/browse/INLONG-163) | Flume sink for TubeMQ |  Major|
|[INLONG-197](https://issues.apache.org/jira/browse/INLONG-197) | Support TubeMQ connector for Apache Flink |  Major|
|[INLONG-238](https://issues.apache.org/jira/browse/INLONG-238) | Support TubeMQ connector for Apache Spark Streaming |  Major|
|[INLONG-239](https://issues.apache.org/jira/browse/INLONG-239) | support deployment on kubernetes |  Major|

### IMPROVEMENTS:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [INLONG-46](https://issues.apache.org/jira/browse/INLONG-46) | Correct some spelling issues |	Low|
| [INLONG-53](https://issues.apache.org/jira/browse/INLONG-53) | fix some typos |	Low|
| [INLONG-55](https://issues.apache.org/jira/browse/INLONG-55) | fix some typos |	Low|
| [INLONG-57](https://issues.apache.org/jira/browse/INLONG-57) | fix some typos & todo |	Low|
| [INLONG-58](https://issues.apache.org/jira/browse/INLONG-58) | fix some typos |	Low|
| [INLONG-60](https://issues.apache.org/jira/browse/INLONG-60) | Remove unnecessary synchronized & using IllegalArgumentException instead of IllegalStateException |	Low|
| [INLONG-61](https://issues.apache.org/jira/browse/INLONG-61) | minor update & fix some typos |	Low|
| [INLONG-64](https://issues.apache.org/jira/browse/INLONG-64) | minor update & fix some typos |	Low|
| [INLONG-67](https://issues.apache.org/jira/browse/INLONG-67) | remove synchronized & fix some typos |	Low|
| [INLONG-71](https://issues.apache.org/jira/browse/INLONG-71) | using IllegalArgumentException & fix some typos |	Low|
| [INLONG-73](https://issues.apache.org/jira/browse/INLONG-73) | remove duplicate codes & some minor updates |	Normal|
| [INLONG-74](https://issues.apache.org/jira/browse/INLONG-74) | minor updates for DefaultBdbStoreService |	Low|
| [INLONG-75](https://issues.apache.org/jira/browse/INLONG-75) | remove unused Logger |	Major|
| [INLONG-76](https://issues.apache.org/jira/browse/INLONG-76) | rename the classes |	Low|
| [INLONG-77](https://issues.apache.org/jira/browse/INLONG-77) | fix typo |	Low|
| [INLONG-79](https://issues.apache.org/jira/browse/INLONG-79) | fix typo |	Major|
| [INLONG-80](https://issues.apache.org/jira/browse/INLONG-80) | Fix some typos |	Low|
| [INLONG-82](https://issues.apache.org/jira/browse/INLONG-82) | Fix some typos & update comments |	Low|
| [INLONG-83](https://issues.apache.org/jira/browse/INLONG-83) | Fix some typos |	Low|
| [INLONG-87](https://issues.apache.org/jira/browse/INLONG-87) | Minor updates |	Low|
| [INLONG-89](https://issues.apache.org/jira/browse/INLONG-89) | Minor updates |	Low|
| [INLONG-90](https://issues.apache.org/jira/browse/INLONG-90) | Remove unused codes in TubeBroker |	Normal|
| [INLONG-91](https://issues.apache.org/jira/browse/INLONG-91) | replace explicit type with <> |	Low|
| [INLONG-93](https://issues.apache.org/jira/browse/INLONG-93) | Substitute the parameterized type for client module & missed server module |	Low|
| [INLONG-94](https://issues.apache.org/jira/browse/INLONG-94) | Substitute the parameterized type for core module |	Low|
| [INLONG-95](https://issues.apache.org/jira/browse/INLONG-95) | Substitute the parameterized type for server module |	Low|
| [INLONG-96](https://issues.apache.org/jira/browse/INLONG-96) | Fix typo & use IllegalArgumentException |	Low|
| [INLONG-98](https://issues.apache.org/jira/browse/INLONG-98) | Fix typo & Simplify 'instanceof' judgment |	Low|
| [INLONG-100](https://issues.apache.org/jira/browse/INLONG-100) | Fix typos & remove unused codes |	Low|
| [INLONG-101](https://issues.apache.org/jira/browse/INLONG-101) | Optimize code & Fix type |	Low|
| [INLONG-103](https://issues.apache.org/jira/browse/INLONG-103) | Substitute Chinese comments with English |	Normal|
| [INLONG-108](https://issues.apache.org/jira/browse/INLONG-108) | About maven jdk version configuration problem |	Minor|
| [INLONG-127](https://issues.apache.org/jira/browse/INLONG-127) | Fixed a bug & minor changes |	Low|
| [INLONG-128](https://issues.apache.org/jira/browse/INLONG-128) | Shorten the log clearup check cycle |	Major|
| [INLONG-138](https://issues.apache.org/jira/browse/INLONG-138) | Optimize core module test case code |	Low|
| [INLONG-141](https://issues.apache.org/jira/browse/INLONG-141) | Remove the requirement to provide localHostIP |	Major|
| [INLONG-152](https://issues.apache.org/jira/browse/INLONG-152) | Modify the master.ini file's annotations |	Normal|
| [INLONG-154](https://issues.apache.org/jira/browse/INLONG-154) | Modify the wrong comment & Minor changes for example module |	Low|
| [INLONG-155](https://issues.apache.org/jira/browse/INLONG-155) | Use enum class for consume position |	Normal|
| [INLONG-156](https://issues.apache.org/jira/browse/INLONG-156) | Update for README.md |	Normal|
| [INLONG-166](https://issues.apache.org/jira/browse/INLONG-166) | Hide `bdbStore` configs in master.ini |	Major|
| [INLONG-167](https://issues.apache.org/jira/browse/INLONG-167) | Change to relative paths in default configs |	Trivial|
| [INLONG-168](https://issues.apache.org/jira/browse/INLONG-168) | Example module: remove localhost IP configuration parameters |	Minor|
| [INLONG-170](https://issues.apache.org/jira/browse/INLONG-170) | improve build/deployment/configuration for quick start |	Major|
| [INLONG-196](https://issues.apache.org/jira/browse/INLONG-196) | use log to print exception |	Low|
| [INLONG-201](https://issues.apache.org/jira/browse/INLONG-201) | [Website] Adjust user guide & fix demo example |	Major|
| [INLONG-202](https://issues.apache.org/jira/browse/INLONG-202) | Add protobuf protocol syntax declaration |	Major|
| [INLONG-213](https://issues.apache.org/jira/browse/INLONG-213) | Optimize code & Minor changes |	Low|
| [INLONG-216](https://issues.apache.org/jira/browse/INLONG-216) | use ThreadUtil.sleep replace Thread.sleep |	Low|
| [INLONG-222](https://issues.apache.org/jira/browse/INLONG-222) | Optimize code: Unnecessary boxing/unboxing conversion |	Normal|
| [INLONG-224](https://issues.apache.org/jira/browse/INLONG-224) | Fixed: Unnecessary conversion to string inspection for server module |	Low|
| [INLONG-226](https://issues.apache.org/jira/browse/INLONG-226) | Add Windows startup scripts |	High|
| [INLONG-227](https://issues.apache.org/jira/browse/INLONG-227) | remove build guide in docker-build readme |	Major|
| [INLONG-232](https://issues.apache.org/jira/browse/INLONG-232) | TubeBroker#register2Master, reconnect and wait |	Low|
| [INLONG-234](https://issues.apache.org/jira/browse/INLONG-234) | Add .asf.yaml to change notifications |	Major|
| [INLONG-235](https://issues.apache.org/jira/browse/INLONG-235) | Add code coverage supporting for pull request created. |	Normal|
| [INLONG-237](https://issues.apache.org/jira/browse/INLONG-237) | add maven module build for docker image |	Major|

### BUG FIXES:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [INLONG-47](https://issues.apache.org/jira/browse/INLONG-47) | Fix some typos |	Major|
| [INLONG-102](https://issues.apache.org/jira/browse/INLONG-102) | Fix question [INLONG-101] [Optimize code] |	Major|
| [INLONG-121](https://issues.apache.org/jira/browse/INLONG-121) | Fix compilation alarm |	Major|
| [INLONG-139](https://issues.apache.org/jira/browse/INLONG-139) | a bug in the equals method of the TubeClientConfig class |	Major|
| [INLONG-157](https://issues.apache.org/jira/browse/INLONG-157) | Optimize Broker disk anomaly check |	Normal|
| [INLONG-158](https://issues.apache.org/jira/browse/INLONG-158) | nextWithAuthInfo2B status should be managed independently according to Broker |	Normal|
| [INLONG-159](https://issues.apache.org/jira/browse/INLONG-159) | Fix some typos |	Normal|
| [INLONG-165](https://issues.apache.org/jira/browse/INLONG-165) | Remove unnecessary fiiles |	Major|
| [INLONG-205](https://issues.apache.org/jira/browse/INLONG-205) | Duplicate dependency of jetty in tuber-server pom file |	Minor|
| [INLONG-206](https://issues.apache.org/jira/browse/INLONG-206) | There are some residual files after executed unit tests |	Major|
| [INLONG-210](https://issues.apache.org/jira/browse/INLONG-210) | Add log4j properties file for unit tests |	Minor|
| [INLONG-217](https://issues.apache.org/jira/browse/INLONG-217) | UPdate the je download path |	Major|
| [INLONG-218](https://issues.apache.org/jira/browse/INLONG-218) | build failed: Too many files with unapproved license |	Major|
| [INLONG-230](https://issues.apache.org/jira/browse/INLONG-230) | TubeMQ run mvn test failed with openjdk version 13.0.2 |	Major|
| [INLONG-236](https://issues.apache.org/jira/browse/INLONG-236) | Can't get dependencies from the maven repository |	Major|
| [INLONG-253](https://issues.apache.org/jira/browse/INLONG-253) | tube-consumer fetch-worker cpu used too high |	Major|
| [INLONG-254](https://issues.apache.org/jira/browse/INLONG-254) | support using different mapping port for standalone mode |	Major|
| [INLONG-265](https://issues.apache.org/jira/browse/INLONG-265) | Unexpected broker disappearance in broker list after updating default broker metadata |	Major|

### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[INLONG-193](https://issues.apache.org/jira/browse/INLONG-193)  | Update project document content |  Major |

### SUB-TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[INLONG-123](https://issues.apache.org/jira/browse/INLONG-123) | Batch flush data to disk |  Major |
|[INLONG-126](https://issues.apache.org/jira/browse/INLONG-126) | Increase the unflushed data bytes control |  Major |
|[INLONG-140](https://issues.apache.org/jira/browse/INLONG-140) | Remove the SSD auxiliary consumption function |  Major |
|[INLONG-160](https://issues.apache.org/jira/browse/INLONG-160) | Improve the protocol between Broker and Master |  Major |
|[INLONG-169](https://issues.apache.org/jira/browse/INLONG-169) | support build with docker image |  Major |
|[INLONG-171](https://issues.apache.org/jira/browse/INLONG-171) | master and broker support config hostname with “localhost” or "127.0.0.1" or dns address |  Major |
|[INLONG-172](https://issues.apache.org/jira/browse/INLONG-172) | simplify start/stop script |  Major |
|[INLONG-173](https://issues.apache.org/jira/browse/INLONG-173) | change jvm memory parameters for default deployment |  Major |
|[INLONG-174](https://issues.apache.org/jira/browse/INLONG-174) | hange defaule accessing url of web gui to http://your-master-ip:8080 |  Major |
|[INLONG-178](https://issues.apache.org/jira/browse/INLONG-178) | change default IPs configuration to localhost |  Major |
|[INLONG-188](https://issues.apache.org/jira/browse/INLONG-188) | the example for demo topic catch exception |  Major |
|[INLONG-194](https://issues.apache.org/jira/browse/INLONG-194) | [website]Remove SSD auxiliary storage introduction |  Major |
|[INLONG-195](https://issues.apache.org/jira/browse/INLONG-195) | [website] Adjust the content of the Chinese part of the document |  Major |
|[INLONG-198](https://issues.apache.org/jira/browse/INLONG-198) | Support TubeMQ source for flink |  Major |
|[INLONG-199](https://issues.apache.org/jira/browse/INLONG-199) | Support TubeMQ sink for flink |  Major |
|[INLONG-204](https://issues.apache.org/jira/browse/INLONG-204) | Remove document address guideline |  Major |
|[INLONG-221](https://issues.apache.org/jira/browse/INLONG-221) | make quick start doc more easy for reading |  Major |
|[INLONG-240](https://issues.apache.org/jira/browse/INLONG-240) | add status command for broker/master script |  Major |
|[INLONG-241](https://issues.apache.org/jira/browse/INLONG-241) | add helm chart for tubemq |  Major |
|[INLONG-242](https://issues.apache.org/jira/browse/INLONG-242) | Support Table interface for TubeMQ flink connector |  Major |
|[INLONG-244](https://issues.apache.org/jira/browse/INLONG-244) | tubemq web support access using proxy IP |  Major |
|[INLONG-246](https://issues.apache.org/jira/browse/INLONG-246) | support register broker using hostname |  Major |
|[INLONG-295](https://issues.apache.org/jira/browse/INLONG-295) | Modify CHANGES.md to add 0.5.0 version release modification |  Major |
|[INLONG-299](https://issues.apache.org/jira/browse/INLONG-299) | Fix RAT check warnning |  Major |
|[INLONG-300](https://issues.apache.org/jira/browse/INLONG-300) | Update LICENSE |  Major |



## Release 0.3.0-incubating - Released (as of 2020-06-08)

### NEW FEATURES:

| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[INLONG-42](https://issues.apache.org/jira/browse/INLONG-42) | Add peer information about message received	Major	New Feature |  Major|

### IMPROVEMENTS:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [INLONG-16](https://issues.apache.org/jira/browse/INLONG-16) |Correct BdbStoreService#isPrimaryNodeActived to BdbStoreService#isPrimaryNodeActive|	Low|
| [INLONG-18](https://issues.apache.org/jira/browse/INLONG-18) |Correct TMaster#idGenerater to TMaster#idGenerator|	Low|
| [INLONG-19](https://issues.apache.org/jira/browse/INLONG-19) |Correct parameter names to fit in camel case|	Low|
| [INLONG-20](https://issues.apache.org/jira/browse/INLONG-20) |Correct DefaultLoadBalancer#balance parameter	| Low|
| [INLONG-21](https://issues.apache.org/jira/browse/INLONG-21) |Change version number from x.y-SNAPSHOT to x.y.z-incubating-SNAPSHOT|	Normal|
| [INLONG-22](https://issues.apache.org/jira/browse/INLONG-22) |Correct ClientSubInfo#getTopicProcesser -> ClientSubInfo#getTopicProcessor|	Low|
| [INLONG-23](https://issues.apache.org/jira/browse/INLONG-23) |Improve project README content introduction|	Major|
| [INLONG-24](https://issues.apache.org/jira/browse/INLONG-24) |Add NOTICE and adjust LICENSE	| Major|
| [INLONG-26](https://issues.apache.org/jira/browse/INLONG-26) |correct spelling (difftime-> diffTime)	|Low|
| [INLONG-27](https://issues.apache.org/jira/browse/INLONG-27) |replace StringBuffer with StringBuilder |	Major|
| [INLONG-28](https://issues.apache.org/jira/browse/INLONG-28) |ignore path error	|Major|
| [INLONG-29](https://issues.apache.org/jira/browse/INLONG-29) |Change the package name to org.apache.tubemq.""	|Major|
| [INLONG-33](https://issues.apache.org/jira/browse/INLONG-33) |refactor enum implement from annoymouse inner class	| Major|
| [INLONG-38](https://issues.apache.org/jira/browse/INLONG-38) |Add Broker's running status check	| Major||
| [INLONG-39](https://issues.apache.org/jira/browse/INLONG-39) |Optimize the loadMessageStores() logic	| Nor|mal|
| [INLONG-40](https://issues.apache.org/jira/browse/INLONG-40) |Optimize message disk store classes's logic	| Major|
| [INLONG-43](https://issues.apache.org/jira/browse/INLONG-43) |Add DeletePolicy's value check	| Major|
| [INLONG-44](https://issues.apache.org/jira/browse/INLONG-44) |Remove unnecessary synchronized definition of shutdown () function	| Normal|
| [INLONG-49](https://issues.apache.org/jira/browse/INLONG-49) |setTimeoutTime change to updTimeoutTime	| Major|
| [INLONG-50](https://issues.apache.org/jira/browse/INLONG-50) |Replace fastjson to gson	| Major|
| [INLONG-7](https://issues.apache.org/jira/browse/INLONG-7) | Using StringBuilder instead of StringBuffer in BaseResult	| Low|
| [INLONG-9](https://issues.apache.org/jira/browse/INLONG-9) | Remove some unnecessary code	| Minor |

### BUG FIXES:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [INLONG-10](https://issues.apache.org/jira/browse/INLONG-10) |Fix Javadoc error|Low|
| [INLONG-14](https://issues.apache.org/jira/browse/INLONG-14) |Some compilation errors|Major|
| [INLONG-15](https://issues.apache.org/jira/browse/INLONG-15) |Correct typo in http_access_API_definition.md|Low|
| [INLONG-32](https://issues.apache.org/jira/browse/INLONG-32) |File path not match with package name in tubemq-client module|Major|
| [INLONG-35](https://issues.apache.org/jira/browse/INLONG-35) |check illegal package's field value|Normal|
| [INLONG-36](https://issues.apache.org/jira/browse/INLONG-36) |Remove unnecessary removefirst() function printing|Normal|
| [INLONG-37](https://issues.apache.org/jira/browse/INLONG-37) |Offset is set to 0 when Broker goes offline|Major|
| [INLONG-45](https://issues.apache.org/jira/browse/INLONG-45) |Check groupName with checkHostName function|Major|
| [INLONG-48](https://issues.apache.org/jira/browse/INLONG-48) |No timeout when setting consumer timeout|Major|
| [INLONG-59](https://issues.apache.org/jira/browse/INLONG-59) |Null pointer exception is thrown while constructing ConsumerConfig with MasterInfo|Normal|
| [INLONG-62](https://issues.apache.org/jira/browse/INLONG-62) |consumed and set consumerConfig.setConsumeModel (0) for the first time|Major|
| [INLONG-66](https://issues.apache.org/jira/browse/INLONG-66) |TubeSingleSessionFactory shutdown bug|Normal|
| [INLONG-85](https://issues.apache.org/jira/browse/INLONG-85) |There is NPE when creating PullConsumer with TubeSingleSessionFactory|Major|
| [INLONG-88](https://issues.apache.org/jira/browse/INLONG-88) |Broker does not take effect after the deletePolicy value is changed|Major|
| [INLONG-149](https://issues.apache.org/jira/browse/INLONG-149) |Some of the consumers stop consuming their corresponding partitions and never release the partition to others|Major|
| [INLONG-153](https://issues.apache.org/jira/browse/INLONG-153) |update copyright notices year to 2020|  Major |
| [INLONG-165](https://issues.apache.org/jira/browse/INLONG-165) |Remove unnecessary fiiles|  Major |

### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[INLONG-12](https://issues.apache.org/jira/browse/INLONG-12)  |Change to use Apache License V2   |  Major |

### SUB-TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[INLONG-130](https://issues.apache.org/jira/browse/INLONG-130) |Generate CHANGES.md and DISCLAIMER-WIP   |  Major |
|[INLONG-133](https://issues.apache.org/jira/browse/INLONG-133) |Add Apache parent pom |  Major |
|[INLONG-134](https://issues.apache.org/jira/browse/INLONG-134) |add maven-source-plugin for generate source jar|  Major |
|[INLONG-135](https://issues.apache.org/jira/browse/INLONG-135) |Refactoring all pom.xml|  Major |
|[INLONG-136](https://issues.apache.org/jira/browse/INLONG-136) |Add LICENSE/NOTICE/DISCLAIMER-WIP to binary package|  Major |
