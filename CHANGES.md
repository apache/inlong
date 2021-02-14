# TubeMQ Changelog

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

## Release 0.8.0-incubating - Released (as of 2021-01-18)

### IMPROVEMENTS:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-430](https://issues.apache.org/jira/browse/TUBEMQ-430) | Optimizing the implementation of HTTP API for broke  | Major |
| [TUBEMQ-445](https://issues.apache.org/jira/browse/TUBEMQ-445) | Adjust the status check default sleep interval of pullConsumeReadyChkSliceMs  | Major |
| [TUBEMQ-448](https://issues.apache.org/jira/browse/TUBEMQ-448) | Add Committer and PPMC operation process  | Major |
| [TUBEMQ-449](https://issues.apache.org/jira/browse/TUBEMQ-449) | Adjust Example implementation  | Major |
| [TUBEMQ-452](https://issues.apache.org/jira/browse/TUBEMQ-452) | Optimize rebalance performance | Major |
| [TUBEMQ-467](https://issues.apache.org/jira/browse/TUBEMQ-467) | Add WEB APIs of Master and Broker | Major |
| [TUBEMQ-489](https://issues.apache.org/jira/browse/TUBEMQ-489) | Add the maximum message length parameter setting | Major |
| [TUBEMQ-495](https://issues.apache.org/jira/browse/TUBEMQ-495) | Code implementation adjustment based on SpotBugs check | Major |
| [TUBEMQ-511](https://issues.apache.org/jira/browse/TUBEMQ-511) | Replace the conditional operator (?:) with mid()  | Major |
| [TUBEMQ-512](https://issues.apache.org/jira/browse/TUBEMQ-512) | Add package length control based on Topic  | Major |
| [TUBEMQ-515](https://issues.apache.org/jira/browse/TUBEMQ-515) | Add cluster Topic view web api  | Major |

### BUG FIXES:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-437](https://issues.apache.org/jira/browse/TUBEMQ-437) | Fix tubemq table source sink factory instance creating problem | Major |
| [TUBEMQ-441](https://issues.apache.org/jira/browse/TUBEMQ-441) | An error occurred when using the Tubemq class to create a sink table | Major |
| [TUBEMQ-442](https://issues.apache.org/jira/browse/TUBEMQ-442) | Modifying the jvm parameters when the broker starts does not take effect  | Major    |
| [TUBEMQ-443](https://issues.apache.org/jira/browse/TUBEMQ-443) | TubemqSourceFunction class prints too many logs problem | Major |
| [TUBEMQ-446](https://issues.apache.org/jira/browse/TUBEMQ-446) | Small bugs fix that do not affect the main logics | Major |
| [TUBEMQ-450](https://issues.apache.org/jira/browse/TUBEMQ-450) | TubeClientException: Generate producer id failed  | Major |
| [TUBEMQ-453](https://issues.apache.org/jira/browse/TUBEMQ-453) | TubemqSourceFunction class prints too many logs problem | Major |
| [TUBEMQ-506](https://issues.apache.org/jira/browse/TUBEMQ-506) | cmakelist error | Major |
| [TUBEMQ-510](https://issues.apache.org/jira/browse/TUBEMQ-510) | Found a bug in MessageProducerExample class | Major |
| [TUBEMQ-518](https://issues.apache.org/jira/browse/TUBEMQ-518) | fix parameter pass error | Major |
| [TUBEMQ-526](https://issues.apache.org/jira/browse/TUBEMQ-526) | Adjust the packaging script and version check list, remove the "-WIP" tag | Major |
| [TUBEMQ-555](https://issues.apache.org/jira/browse/TUBEMQ-555) | short session data can only be written to a specific partition | Major |
| [TUBEMQ-556](https://issues.apache.org/jira/browse/TUBEMQ-556) | Index value is bigger than the actual number of records | Low |


### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [TUBEMQ-505](https://issues.apache.org/jira/browse/TUBEMQ-505) | Remove the "WIP" label of the DISCLAIMER file  | Major |
| [TUBEMQ-543](https://issues.apache.org/jira/browse/TUBEMQ-543) | Modify the LICENSE statement of multiple files and others  | Major |
| [TUBEMQ-557](https://issues.apache.org/jira/browse/TUBEMQ-557) | Handle the issues mentioned in the 0.8.0-RC2 review  | Major |

### SUB-TASK:
| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-428](https://issues.apache.org/jira/browse/TUBEMQ-433) | Bumped version to 0.8.0-SNAPSHOT | Major |
| [TUBEMQ-433](https://issues.apache.org/jira/browse/TUBEMQ-433) | add tubemq perf-consumer/producer scripts | Major |
| [TUBEMQ-434](https://issues.apache.org/jira/browse/TUBEMQ-434) | Adjust Broker API mapping | Major |
| [TUBEMQ-435](https://issues.apache.org/jira/browse/TUBEMQ-435) | Create Web field Mapping | Major |
| [TUBEMQ-436](https://issues.apache.org/jira/browse/TUBEMQ-436) | Adjust Broker's HTTP API implementation | Major |
| [TUBEMQ-439](https://issues.apache.org/jira/browse/TUBEMQ-439) | Add Cli field Scheme definition | Major |
| [TUBEMQ-440](https://issues.apache.org/jira/browse/TUBEMQ-440) | Add feature package tube-manager to zip  | Major |
| [TUBEMQ-444](https://issues.apache.org/jira/browse/TUBEMQ-444) | Add consume and produce Cli commands | Major |
| [TUBEMQ-447](https://issues.apache.org/jira/browse/TUBEMQ-447) | Add Broker-Admin Cli | Major |
| [TUBEMQ-451](https://issues.apache.org/jira/browse/TUBEMQ-451) | Replace ConsumeTupleInfo with Tuple2  | Major    |
| [TUBEMQ-457](https://issues.apache.org/jira/browse/TUBEMQ-457) | There is no need to return StringBuilder in Master.java | Major |
| [TUBEMQ-463](https://issues.apache.org/jira/browse/TUBEMQ-463) | Adjust Master rebalance process implementation  | Major |
| [TUBEMQ-464](https://issues.apache.org/jira/browse/TUBEMQ-464) | Add parameter rebalanceParallel in master.ini | Major |
| [TUBEMQ-470](https://issues.apache.org/jira/browse/TUBEMQ-470) | Add query API of TopicName and BrokerId collection  | Major |
| [TUBEMQ-471](https://issues.apache.org/jira/browse/TUBEMQ-471) | Add query API Introduction of TopicName and BrokerId collection | Major |
| [TUBEMQ-472](https://issues.apache.org/jira/browse/TUBEMQ-472) | Adjust Broker's AbstractWebHandler class implementation  | Major |
| [TUBEMQ-475](https://issues.apache.org/jira/browse/TUBEMQ-475) | add the offset clone api of the consume group  | Major |
| [TUBEMQ-482](https://issues.apache.org/jira/browse/TUBEMQ-482) | Add offset query api  | Major |
| [TUBEMQ-484](https://issues.apache.org/jira/browse/TUBEMQ-484) | Add query API for topic publication information  | Major |
| [TUBEMQ-485](https://issues.apache.org/jira/browse/TUBEMQ-485) | Add the batch setting API of consume group offset  | Major |
| [TUBEMQ-486](https://issues.apache.org/jira/browse/TUBEMQ-486) | Add the delete API of consumer group offset  | Major |
| [TUBEMQ-494](https://issues.apache.org/jira/browse/TUBEMQ-494) | Update API interface instruction document | Major |
| [TUBEMQ-499](https://issues.apache.org/jira/browse/TUBEMQ-499) | Add configure store  | Major |
| [TUBEMQ-500](https://issues.apache.org/jira/browse/TUBEMQ-500) | Add setting operate API  | Major |
| [TUBEMQ-501](https://issues.apache.org/jira/browse/TUBEMQ-501) | Adjust max message size check logic  | Major |
| [TUBEMQ-502](https://issues.apache.org/jira/browse/TUBEMQ-502) | Add setting API interface document  | Major |
| [TUBEMQ-504](https://issues.apache.org/jira/browse/TUBEMQ-504) | Adjust the WebMethodMapper class interfaces  | Major |
| [TUBEMQ-508](https://issues.apache.org/jira/browse/TUBEMQ-508) | Optimize Broker's PB parameter check processing logic  | Major |
| [TUBEMQ-509](https://issues.apache.org/jira/browse/TUBEMQ-509) | Adjust the packet length check when data is loaded  | Major |
| [TUBEMQ-522](https://issues.apache.org/jira/browse/TUBEMQ-522) | Add admin_query_cluster_topic_view API document  | Major |
| [TUBEMQ-544](https://issues.apache.org/jira/browse/TUBEMQ-544) | Adjust the LICENSE statement in the client.conf files of Python and C/C++ SDK | Major |
| [TUBEMQ-546](https://issues.apache.org/jira/browse/TUBEMQ-546) | Restore the original license header of the referenced external source files | Major |
| [TUBEMQ-547](https://issues.apache.org/jira/browse/TUBEMQ-547) | Recode the implementation of the *Startup.java classes in the Tool package | Major |
| [TUBEMQ-548](https://issues.apache.org/jira/browse/TUBEMQ-548) | Handle the LICENSE authorization of font files in the resources | Major |
| [TUBEMQ-549](https://issues.apache.org/jira/browse/TUBEMQ-549) | Handling the problem of compilation failure | Major |
| [TUBEMQ-550](https://issues.apache.org/jira/browse/TUBEMQ-550) | Adjust LICENSE file content | Major |
| [TUBEMQ-551](https://issues.apache.org/jira/browse/TUBEMQ-551) | Adjust NOTICE file content | Major |
| [TUBEMQ-558](https://issues.apache.org/jira/browse/TUBEMQ-558) | Adjust the LICENSE of the file header | Major |
| [TUBEMQ-559](https://issues.apache.org/jira/browse/TUBEMQ-559) | Update the LICENSE file according to the 0.8.0-RC2 review | Major |
| [TUBEMQ-560](https://issues.apache.org/jira/browse/TUBEMQ-560) | Remove unprepared modules | Major |


## Release 0.7.0-incubating - Released (as of 2020-11-25)

### New Features:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-162](https://issues.apache.org/jira/browse/TUBEMQ-162) | Python SDK support in TubeMQ | High |
| [TUBEMQ-336](https://issues.apache.org/jira/browse/TUBEMQ-336) | Propose web portal to manage tube cluster Phase-I | Major |
| [TUBEMQ-390](https://issues.apache.org/jira/browse/TUBEMQ-390)   | support build C++ SDK with docker image | Normal |

### IMPROVEMENTS:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-369](https://issues.apache.org/jira/browse/TUBEMQ-369) | hope to add an option in the compilation script (like `make lib` etc...)                 | Major    |
| [TUBEMQ-373](https://issues.apache.org/jira/browse/TUBEMQ-373) | Reduce the redundant code of Utils::Split functions             | Major    |
| [TUBEMQ-374](https://issues.apache.org/jira/browse/TUBEMQ-374) | Adjust some coding style issues     | Major    |
| [TUBEMQ-375](https://issues.apache.org/jira/browse/TUBEMQ-375) | Add a section to the README file about how to compile the project| Major    |
| [TUBEMQ-385](https://issues.apache.org/jira/browse/TUBEMQ-385) | update docker images     | Major    |
| [TUBEMQ-393](https://issues.apache.org/jira/browse/TUBEMQ-393) | Optimize the mapping code of WEB API     | Major    |
| [TUBEMQ-406](https://issues.apache.org/jira/browse/TUBEMQ-406) | test_consumer.py works for both Python 2 and 3   | Minor |
| [TUBEMQ-410](https://issues.apache.org/jira/browse/TUBEMQ-410) | install python package and simplify test_consumer.py    | Major    |
| [TUBEMQ-416](https://issues.apache.org/jira/browse/TUBEMQ-416) | support consume from specified position   | Major    |
| [TUBEMQ-417](https://issues.apache.org/jira/browse/TUBEMQ-417) | C++ Client support parse message from binary data for Python SDK    | Major    |
| [TUBEMQ-419](https://issues.apache.org/jira/browse/TUBEMQ-419) | SetMaxPartCheckPeriodMs() negative number, getMessage() still  | Major    |

### BUG FIXES:

| JIRA                                                         | Summary                                                      | Priority |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :------- |
| [TUBEMQ-365](https://issues.apache.org/jira/browse/TUBEMQ-365) | Whether the consumption setting is wrong after the processRequest exception | Major    |
| [TUBEMQ-370](https://issues.apache.org/jira/browse/TUBEMQ-370) | Calling GetCurConsumedInfo API always returns failure      | Major    |
| [TUBEMQ-376](https://issues.apache.org/jira/browse/TUBEMQ-376) | Move pullrequests_status notifications commits mail list | Major    |
| [TUBEMQ-366](https://issues.apache.org/jira/browse/TUBEMQ-366) | Found a nullpointerexception bug in broker | Normal |
| [TUBEMQ-379](https://issues.apache.org/jira/browse/TUBEMQ-379) | Modify the memory cache size default to 3M | Normal |
| [TUBEMQ-380](https://issues.apache.org/jira/browse/TUBEMQ-380) | Cpp client link error when gcc optimization is disabled   | Major    |
| [TUBEMQ-405](https://issues.apache.org/jira/browse/TUBEMQ-405) | python sdk install files lack of the whole cpp configuration | Major |
| [TUBEMQ-401](https://issues.apache.org/jira/browse/TUBEMQ-401) | python sdk readme bug | Minor |
| [TUBEMQ-407](https://issues.apache.org/jira/browse/TUBEMQ-407) | Fix some content in README.md | Trivial |
| [TUBEMQ-418](https://issues.apache.org/jira/browse/TUBEMQ-418) | C++ SDK function SetMaxPartCheckPeriodMs() can't work | Major |

### SUB-TASK:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-276](https://issues.apache.org/jira/browse/TUBEMQ-276) | add python client encode/decode protobuf message for TubeMQ RPC Protocol                                 | Major    |
| [TUBEMQ-338](https://issues.apache.org/jira/browse/TUBEMQ-338) | web pages for tubemq manager                                     | Major    |
| [TUBEMQ-341](https://issues.apache.org/jira/browse/TUBEMQ-341) | open independent sub-project for tubemq                                | Major    |
| [TUBEMQ-342](https://issues.apache.org/jira/browse/TUBEMQ-342) | abstract backend threads for routine management                              | Major    |
| [TUBEMQ-346](https://issues.apache.org/jira/browse/TUBEMQ-346) | remove chinese comments                                          | Minor |
| [TUBEMQ-355](https://issues.apache.org/jira/browse/TUBEMQ-355) | Add business entity for topic manager                            | Major    |
| [TUBEMQ-361](https://issues.apache.org/jira/browse/TUBEMQ-361) | create topic when getting request             | Major    |
| [TUBEMQ-364](https://issues.apache.org/jira/browse/TUBEMQ-364) | uniform response format for exception state                              | Major    |
| [TUBEMQ-383](https://issues.apache.org/jira/browse/TUBEMQ-383) | document about Installation/API Reference/Example                                   | Major    |
| [TUBEMQ-387](https://issues.apache.org/jira/browse/TUBEMQ-387) | add manager web pages                                       | Major    |
| [TUBEMQ-392](https://issues.apache.org/jira/browse/TUBEMQ-392) | add query rest api for clusters| Major    |
| [TUBEMQ-394](https://issues.apache.org/jira/browse/TUBEMQ-394) | Creating Mapper class from web api to inner handler | Major    |
| [TUBEMQ-395](https://issues.apache.org/jira/browse/TUBEMQ-395) | Create Abstract WebHandler class                            | Major    |
| [TUBEMQ-396](https://issues.apache.org/jira/browse/TUBEMQ-396) | Adjust the WebXXXHandler classes implementation  | Major    |
| [TUBEMQ-397](https://issues.apache.org/jira/browse/TUBEMQ-397) | Add master info and other info web handler   | Major    |
| [TUBEMQ-398](https://issues.apache.org/jira/browse/TUBEMQ-398) | reinit project for using pybind11                            | Major    |
| [TUBEMQ-399](https://issues.apache.org/jira/browse/TUBEMQ-399) | expose C++ SDK method by Pybind11                                         | Major    |
| [TUBEMQ-400](https://issues.apache.org/jira/browse/TUBEMQ-400) | add example for consume message by bypind11                                 | Major    |
| [TUBEMQ-402](https://issues.apache.org/jira/browse/TUBEMQ-402) | add modify rest api for clusters                           | Major    |
| [TUBEMQ-412](https://issues.apache.org/jira/browse/TUBEMQ-402) | tube manager start stop scrrpts                           | Major    |
| [TUBEMQ-415](https://issues.apache.org/jira/browse/TUBEMQ-415) | exclude apache license for front end code  | Major    |

## Release 0.6.0-incubating - Released (as of 2020-09-25)

### New Features:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-319](https://issues.apache.org/jira/browse/TUBEMQ-319) | In the pull mode, consumers support the  suspension of consumption for a certain partition | Major    |
| [TUBEMQ-3](https://issues.apache.org/jira/browse/TUBEMQ-3)   | C++ SDK support in TubeMQ                                    | Normal   |

### IMPROVEMENTS:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-311](https://issues.apache.org/jira/browse/TUBEMQ-311) | Feedback more production information                 | Major    |
| [TUBEMQ-312](https://issues.apache.org/jira/browse/TUBEMQ-312) | Feedback more consumption information                | Major    |
| [TUBEMQ-325](https://issues.apache.org/jira/browse/TUBEMQ-325) | Add 406 ~ 408 error code to pullSelect call          | Major    |
| [TUBEMQ-345](https://issues.apache.org/jira/browse/TUBEMQ-345) | Optimize the call logic of getMessage() in Pull mode | Major    |
| [TUBEMQ-352](https://issues.apache.org/jira/browse/TUBEMQ-352) | Set the parameters of the example at startup         | Major    |
| [TUBEMQ-353](https://issues.apache.org/jira/browse/TUBEMQ-353) | Update LICENSE about C/C++ SDK's code reference      | Major    |
| [TUBEMQ-356](https://issues.apache.org/jira/browse/TUBEMQ-356) | C++ SDK Codec decode add requestid                   | Major    |
| [TUBEMQ-327](https://issues.apache.org/jira/browse/TUBEMQ-327) | Fix the concurrency problem in the example           | Normal   |

### BUG FIXES:

| JIRA                                                         | Summary                                                      | Priority |
| :----------------------------------------------------------- | :----------------------------------------------------------- | :------- |
| [TUBEMQ-316](https://issues.apache.org/jira/browse/TUBEMQ-316) | Where the port the port is aleady used, the  process throw the exception, but not exit | Major    |
| [TUBEMQ-317](https://issues.apache.org/jira/browse/TUBEMQ-317) | The Store Manager throws java.lang.NullPointerException      | Major    |
| [TUBEMQ-320](https://issues.apache.org/jira/browse/TUBEMQ-320) | Request for static web contents would get responses with no content | Major    |
| [TUBEMQ-354](https://issues.apache.org/jira/browse/TUBEMQ-354) | Found a dns translate bug in C/C++ sdk                       | Major    |
| [TUBEMQ-306](https://issues.apache.org/jira/browse/TUBEMQ-306) | Raise Nullpointer Exception when create tubemq instance      | Low      |
| [TUBEMQ-359](https://issues.apache.org/jira/browse/TUBEMQ-359) | TubeMQ consume speed dropped to 0 in some partitions, it is a very serious bug | Blocker  |

### SUB-TASK:

| JIRA  | Summary  | Priority |
| :---- | :------- | :------- |
| [TUBEMQ-250](https://issues.apache.org/jira/browse/TUBEMQ-250) | Create C/C++ configure files                                 | Major    |
| [TUBEMQ-251](https://issues.apache.org/jira/browse/TUBEMQ-251) | Create C/C++ Codec utils                                     | Major    |
| [TUBEMQ-252](https://issues.apache.org/jira/browse/TUBEMQ-252) | Create C/C++ Metadata classes                                | Major    |
| [TUBEMQ-262](https://issues.apache.org/jira/browse/TUBEMQ-262) | Create C++ flow control handler                              | Major    |
| [TUBEMQ-263](https://issues.apache.org/jira/browse/TUBEMQ-263) | Create C/C++ ini file read utils                             | Major    |
| [TUBEMQ-266](https://issues.apache.org/jira/browse/TUBEMQ-266) | [TUBEMQ-266] Add Tencent/rapidjson as submodule              | Major    |
| [TUBEMQ-267](https://issues.apache.org/jira/browse/TUBEMQ-267) | Create C/C++ Message class                                   | Major    |
| [TUBEMQ-269](https://issues.apache.org/jira/browse/TUBEMQ-269) | Create C/C++ RmtDataCache class                              | Major    |
| [TUBEMQ-272](https://issues.apache.org/jira/browse/TUBEMQ-272) | Unified C/C++ files's code style                             | Major    |
| [TUBEMQ-274](https://issues.apache.org/jira/browse/TUBEMQ-274) | Support CMake compilation                                    | Major    |
| [TUBEMQ-275](https://issues.apache.org/jira/browse/TUBEMQ-275) | Thread Pool & Timer                                          | Major    |
| [TUBEMQ-280](https://issues.apache.org/jira/browse/TUBEMQ-280) | Create C/C++ subscribe info class                            | Major    |
| [TUBEMQ-281](https://issues.apache.org/jira/browse/TUBEMQ-281) | atomic_def.h use C++11 stdlib class                          | Major    |
| [TUBEMQ-282](https://issues.apache.org/jira/browse/TUBEMQ-282) | Create C/C++ return result class                             | Major    |
| [TUBEMQ-283](https://issues.apache.org/jira/browse/TUBEMQ-283) | Adjust C/C++ some file names: add "tubemq_" prefix           | Major    |
| [TUBEMQ-285](https://issues.apache.org/jira/browse/TUBEMQ-285) | Replace C/C++ pthread's mutex to std::mutex                  | Major    |
| [TUBEMQ-286](https://issues.apache.org/jira/browse/TUBEMQ-286) | Create C/C++ SDK's manager class                             | Major    |
| [TUBEMQ-287](https://issues.apache.org/jira/browse/TUBEMQ-287) | C++ SDK io buffer                                            | Major    |
| [TUBEMQ-288](https://issues.apache.org/jira/browse/TUBEMQ-288) | C++ SDK Codec interface                                      | Major    |
| [TUBEMQ-289](https://issues.apache.org/jira/browse/TUBEMQ-289) | C++ SDK Codec TubeMQ proto support                           | Major    |
| [TUBEMQ-290](https://issues.apache.org/jira/browse/TUBEMQ-290) | C++ SDK TCP Connect                                          | Major    |
| [TUBEMQ-291](https://issues.apache.org/jira/browse/TUBEMQ-291) | C++ SDK Connect Pool                                         | Major    |
| [TUBEMQ-293](https://issues.apache.org/jira/browse/TUBEMQ-293) | C++ SDK Create Future class                                  | Major    |
| [TUBEMQ-296](https://issues.apache.org/jira/browse/TUBEMQ-296) | Adjust the version information of all pom.xml                | Major    |
| [TUBEMQ-300](https://issues.apache.org/jira/browse/TUBEMQ-300) | Update LICENSE                                               | Major    |
| [TUBEMQ-308](https://issues.apache.org/jira/browse/TUBEMQ-308) | Upgrade Jetty 6 (mortbay) => Jetty 9 (eclipse)               | Major    |
| [TUBEMQ-309](https://issues.apache.org/jira/browse/TUBEMQ-309) | Add POST support to WebAPI                                   | Major    |
| [TUBEMQ-326](https://issues.apache.org/jira/browse/TUBEMQ-326) | [website] Added 405 ~ 408 error code definition              | Major    |
| [TUBEMQ-347](https://issues.apache.org/jira/browse/TUBEMQ-347) | C++ SDK Create client API                                    | Major    |
| [TUBEMQ-348](https://issues.apache.org/jira/browse/TUBEMQ-348) | C++SDK Client handler detail                                 | Major    |
| [TUBEMQ-349](https://issues.apache.org/jira/browse/TUBEMQ-349) | C++ SDK Create Thread Pool                                   | Major    |
| [TUBEMQ-350](https://issues.apache.org/jira/browse/TUBEMQ-350) | C++ SDK client code adj                                      | Major    |
| [TUBEMQ-351](https://issues.apache.org/jira/browse/TUBEMQ-351) | C++ SDK example&tests                                        | Major    |
| [TUBEMQ-358](https://issues.apache.org/jira/browse/TUBEMQ-358) | Adjust tubemq-manager, remove it from master, and develop with TUBEMQ-336  branch | Major    |
| [TUBEMQ-268](https://issues.apache.org/jira/browse/TUBEMQ-268) | C++ SDK log module                                           | Normal   |
| [TUBEMQ-292](https://issues.apache.org/jira/browse/TUBEMQ-292) | C++ SDK singleton & executor_pool optimization               | Normal   |
| [TUBEMQ-270](https://issues.apache.org/jira/browse/TUBEMQ-270) | this point c++ SDK class                                     | Minor    |
| [TUBEMQ-271](https://issues.apache.org/jira/browse/TUBEMQ-271) | C++ SDK copy constructor and  assignment constructor         | Minor    |
| [TUBEMQ-273](https://issues.apache.org/jira/browse/TUBEMQ-273) | C++ SDK dir name change inc -> include/tubemq/               | Minor    |

## Release 0.5.0-incubating - released (as of 2020-07-22)

### NEW FEATURES:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[TUBEMQ-122](https://issues.apache.org/jira/browse/TUBEMQ-122) | Increase JAVA version collection of SDK environment |  Major|
|[TUBEMQ-163](https://issues.apache.org/jira/browse/TUBEMQ-163) | Flume sink for TubeMQ |  Major|
|[TUBEMQ-197](https://issues.apache.org/jira/browse/TUBEMQ-197) | Support TubeMQ connector for Apache Flink |  Major|
|[TUBEMQ-238](https://issues.apache.org/jira/browse/TUBEMQ-238) | Support TubeMQ connector for Apache Spark Streaming |  Major|
|[TUBEMQ-239](https://issues.apache.org/jira/browse/TUBEMQ-239) | support deployment on kubernetes |  Major|

### IMPROVEMENTS:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [TUBEMQ-46](https://issues.apache.org/jira/browse/TUBEMQ-46) | Correct some spelling issues |	Low|
| [TUBEMQ-53](https://issues.apache.org/jira/browse/TUBEMQ-53) | fix some typos |	Low|
| [TUBEMQ-55](https://issues.apache.org/jira/browse/TUBEMQ-55) | fix some typos |	Low|
| [TUBEMQ-57](https://issues.apache.org/jira/browse/TUBEMQ-57) | fix some typos & todo |	Low|
| [TUBEMQ-58](https://issues.apache.org/jira/browse/TUBEMQ-58) | fix some typos |	Low|
| [TUBEMQ-60](https://issues.apache.org/jira/browse/TUBEMQ-60) | Remove unnecessary synchronized & using IllegalArgumentException instead of IllegalStateException |	Low|
| [TUBEMQ-61](https://issues.apache.org/jira/browse/TUBEMQ-61) | minor update & fix some typos |	Low|
| [TUBEMQ-64](https://issues.apache.org/jira/browse/TUBEMQ-64) | minor update & fix some typos |	Low|
| [TUBEMQ-67](https://issues.apache.org/jira/browse/TUBEMQ-67) | remove synchronized & fix some typos |	Low|
| [TUBEMQ-71](https://issues.apache.org/jira/browse/TUBEMQ-71) | using IllegalArgumentException & fix some typos |	Low|
| [TUBEMQ-73](https://issues.apache.org/jira/browse/TUBEMQ-73) | remove duplicate codes & some minor updates |	Normal|
| [TUBEMQ-74](https://issues.apache.org/jira/browse/TUBEMQ-74) | minor updates for DefaultBdbStoreService |	Low|
| [TUBEMQ-75](https://issues.apache.org/jira/browse/TUBEMQ-75) | remove unused Logger |	Major|
| [TUBEMQ-76](https://issues.apache.org/jira/browse/TUBEMQ-76) | rename the classes |	Low|
| [TUBEMQ-77](https://issues.apache.org/jira/browse/TUBEMQ-77) | fix typo |	Low|
| [TUBEMQ-79](https://issues.apache.org/jira/browse/TUBEMQ-79) | fix typo |	Major|
| [TUBEMQ-80](https://issues.apache.org/jira/browse/TUBEMQ-80) | Fix some typos |	Low|
| [TUBEMQ-82](https://issues.apache.org/jira/browse/TUBEMQ-82) | Fix some typos & update comments |	Low|
| [TUBEMQ-83](https://issues.apache.org/jira/browse/TUBEMQ-83) | Fix some typos |	Low|
| [TUBEMQ-87](https://issues.apache.org/jira/browse/TUBEMQ-87) | Minor updates |	Low|
| [TUBEMQ-89](https://issues.apache.org/jira/browse/TUBEMQ-89) | Minor updates |	Low|
| [TUBEMQ-90](https://issues.apache.org/jira/browse/TUBEMQ-90) | Remove unused codes in TubeBroker |	Normal|
| [TUBEMQ-91](https://issues.apache.org/jira/browse/TUBEMQ-91) | replace explicit type with <> |	Low|
| [TUBEMQ-93](https://issues.apache.org/jira/browse/TUBEMQ-93) | Substitute the parameterized type for client module & missed server module |	Low|
| [TUBEMQ-94](https://issues.apache.org/jira/browse/TUBEMQ-94) | Substitute the parameterized type for core module |	Low|
| [TUBEMQ-95](https://issues.apache.org/jira/browse/TUBEMQ-95) | Substitute the parameterized type for server module |	Low|
| [TUBEMQ-96](https://issues.apache.org/jira/browse/TUBEMQ-96) | Fix typo & use IllegalArgumentException |	Low|
| [TUBEMQ-98](https://issues.apache.org/jira/browse/TUBEMQ-98) | Fix typo & Simplify 'instanceof' judgment |	Low|
| [TUBEMQ-100](https://issues.apache.org/jira/browse/TUBEMQ-100) | Fix typos & remove unused codes |	Low|
| [TUBEMQ-101](https://issues.apache.org/jira/browse/TUBEMQ-101) | Optimize code & Fix type |	Low|
| [TUBEMQ-103](https://issues.apache.org/jira/browse/TUBEMQ-103) | Substitute Chinese comments with English |	Normal|
| [TUBEMQ-108](https://issues.apache.org/jira/browse/TUBEMQ-108) | About maven jdk version configuration problem |	Minor|
| [TUBEMQ-127](https://issues.apache.org/jira/browse/TUBEMQ-127) | Fixed a bug & minor changes |	Low|
| [TUBEMQ-128](https://issues.apache.org/jira/browse/TUBEMQ-128) | Shorten the log clearup check cycle |	Major|
| [TUBEMQ-138](https://issues.apache.org/jira/browse/TUBEMQ-138) | Optimize core module test case code |	Low|
| [TUBEMQ-141](https://issues.apache.org/jira/browse/TUBEMQ-141) | Remove the requirement to provide localHostIP |	Major|
| [TUBEMQ-152](https://issues.apache.org/jira/browse/TUBEMQ-152) | Modify the master.ini file's annotations |	Normal|
| [TUBEMQ-154](https://issues.apache.org/jira/browse/TUBEMQ-154) | Modify the wrong comment & Minor changes for example module |	Low|
| [TUBEMQ-155](https://issues.apache.org/jira/browse/TUBEMQ-155) | Use enum class for consume position |	Normal|
| [TUBEMQ-156](https://issues.apache.org/jira/browse/TUBEMQ-156) | Update for README.md |	Normal|
| [TUBEMQ-166](https://issues.apache.org/jira/browse/TUBEMQ-166) | Hide `bdbStore` configs in master.ini |	Major|
| [TUBEMQ-167](https://issues.apache.org/jira/browse/TUBEMQ-167) | Change to relative paths in default configs |	Trivial|
| [TUBEMQ-168](https://issues.apache.org/jira/browse/TUBEMQ-168) | Example module: remove localhost IP configuration parameters |	Minor|
| [TUBEMQ-170](https://issues.apache.org/jira/browse/TUBEMQ-170) | improve build/deployment/configuration for quick start |	Major|
| [TUBEMQ-196](https://issues.apache.org/jira/browse/TUBEMQ-196) | use log to print exception |	Low|
| [TUBEMQ-201](https://issues.apache.org/jira/browse/TUBEMQ-201) | [Website] Adjust user guide & fix demo example |	Major|
| [TUBEMQ-202](https://issues.apache.org/jira/browse/TUBEMQ-202) | Add protobuf protocol syntax declaration |	Major|
| [TUBEMQ-213](https://issues.apache.org/jira/browse/TUBEMQ-213) | Optimize code & Minor changes |	Low|
| [TUBEMQ-216](https://issues.apache.org/jira/browse/TUBEMQ-216) | use ThreadUtil.sleep replace Thread.sleep |	Low|
| [TUBEMQ-222](https://issues.apache.org/jira/browse/TUBEMQ-222) | Optimize code: Unnecessary boxing/unboxing conversion |	Normal|
| [TUBEMQ-224](https://issues.apache.org/jira/browse/TUBEMQ-224) | Fixed: Unnecessary conversion to string inspection for server module |	Low|
| [TUBEMQ-226](https://issues.apache.org/jira/browse/TUBEMQ-226) | Add Windows startup scripts |	High|
| [TUBEMQ-227](https://issues.apache.org/jira/browse/TUBEMQ-227) | remove build guide in docker-build readme |	Major|
| [TUBEMQ-232](https://issues.apache.org/jira/browse/TUBEMQ-232) | TubeBroker#register2Master, reconnect and wait |	Low|
| [TUBEMQ-234](https://issues.apache.org/jira/browse/TUBEMQ-234) | Add .asf.yaml to change notifications |	Major|
| [TUBEMQ-235](https://issues.apache.org/jira/browse/TUBEMQ-235) | Add code coverage supporting for pull request created. |	Normal|
| [TUBEMQ-237](https://issues.apache.org/jira/browse/TUBEMQ-237) | add maven module build for docker image |	Major|

### BUG FIXES:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [TUBEMQ-47](https://issues.apache.org/jira/browse/TUBEMQ-47) | Fix some typos |	Major|
| [TUBEMQ-102](https://issues.apache.org/jira/browse/TUBEMQ-102) | Fix question [TUBEMQ-101] [Optimize code] |	Major|
| [TUBEMQ-121](https://issues.apache.org/jira/browse/TUBEMQ-121) | Fix compilation alarm |	Major|
| [TUBEMQ-139](https://issues.apache.org/jira/browse/TUBEMQ-139) | a bug in the equals method of the TubeClientConfig class |	Major|
| [TUBEMQ-157](https://issues.apache.org/jira/browse/TUBEMQ-157) | Optimize Broker disk anomaly check |	Normal|
| [TUBEMQ-158](https://issues.apache.org/jira/browse/TUBEMQ-158) | nextWithAuthInfo2B status should be managed independently according to Broker |	Normal|
| [TUBEMQ-159](https://issues.apache.org/jira/browse/TUBEMQ-159) | Fix some typos |	Normal|
| [TUBEMQ-165](https://issues.apache.org/jira/browse/TUBEMQ-165) | Remove unnecessary fiiles |	Major|
| [TUBEMQ-205](https://issues.apache.org/jira/browse/TUBEMQ-205) | Duplicate dependency of jetty in tuber-server pom file |	Minor|
| [TUBEMQ-206](https://issues.apache.org/jira/browse/TUBEMQ-206) | There are some residual files after executed unit tests |	Major|
| [TUBEMQ-210](https://issues.apache.org/jira/browse/TUBEMQ-210) | Add log4j properties file for unit tests |	Minor|
| [TUBEMQ-217](https://issues.apache.org/jira/browse/TUBEMQ-217) | UPdate the je download path |	Major|
| [TUBEMQ-218](https://issues.apache.org/jira/browse/TUBEMQ-218) | build failed: Too many files with unapproved license |	Major|
| [TUBEMQ-230](https://issues.apache.org/jira/browse/TUBEMQ-230) | TubeMQ run mvn test failed with openjdk version 13.0.2 |	Major|
| [TUBEMQ-236](https://issues.apache.org/jira/browse/TUBEMQ-236) | Can't get dependencies from the maven repository |	Major|
| [TUBEMQ-253](https://issues.apache.org/jira/browse/TUBEMQ-253) | tube-consumer fetch-worker cpu used too high |	Major|
| [TUBEMQ-254](https://issues.apache.org/jira/browse/TUBEMQ-254) | support using different mapping port for standalone mode |	Major|
| [TUBEMQ-265](https://issues.apache.org/jira/browse/TUBEMQ-265) | Unexpected broker disappearance in broker list after updating default broker metadata |	Major|

### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[TUBEMQ-193](https://issues.apache.org/jira/browse/TUBEMQ-193)  | Update project document content |  Major |

### SUB-TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[TUBEMQ-123](https://issues.apache.org/jira/browse/TUBEMQ-123) | Batch flush data to disk |  Major |
|[TUBEMQ-126](https://issues.apache.org/jira/browse/TUBEMQ-126) | Increase the unflushed data bytes control |  Major |
|[TUBEMQ-140](https://issues.apache.org/jira/browse/TUBEMQ-140) | Remove the SSD auxiliary consumption function |  Major |
|[TUBEMQ-160](https://issues.apache.org/jira/browse/TUBEMQ-160) | Improve the protocol between Broker and Master |  Major |
|[TUBEMQ-169](https://issues.apache.org/jira/browse/TUBEMQ-169) | support build with docker image |  Major |
|[TUBEMQ-171](https://issues.apache.org/jira/browse/TUBEMQ-171) | master and broker support config hostname with “localhost” or "127.0.0.1" or dns address |  Major |
|[TUBEMQ-172](https://issues.apache.org/jira/browse/TUBEMQ-172) | simplify start/stop script |  Major |
|[TUBEMQ-173](https://issues.apache.org/jira/browse/TUBEMQ-173) | change jvm memory parameters for default deployment |  Major |
|[TUBEMQ-174](https://issues.apache.org/jira/browse/TUBEMQ-174) | hange defaule accessing url of web gui to http://your-master-ip:8080 |  Major |
|[TUBEMQ-178](https://issues.apache.org/jira/browse/TUBEMQ-178) | change default IPs configuration to localhost |  Major |
|[TUBEMQ-188](https://issues.apache.org/jira/browse/TUBEMQ-188) | the example for demo topic catch exception |  Major |
|[TUBEMQ-194](https://issues.apache.org/jira/browse/TUBEMQ-194) | [website]Remove SSD auxiliary storage introduction |  Major |
|[TUBEMQ-195](https://issues.apache.org/jira/browse/TUBEMQ-195) | [website] Adjust the content of the Chinese part of the document |  Major |
|[TUBEMQ-198](https://issues.apache.org/jira/browse/TUBEMQ-198) | Support TubeMQ source for flink |  Major |
|[TUBEMQ-199](https://issues.apache.org/jira/browse/TUBEMQ-199) | Support TubeMQ sink for flink |  Major |
|[TUBEMQ-204](https://issues.apache.org/jira/browse/TUBEMQ-204) | Remove document address guideline |  Major |
|[TUBEMQ-221](https://issues.apache.org/jira/browse/TUBEMQ-221) | make quick start doc more easy for reading |  Major |
|[TUBEMQ-240](https://issues.apache.org/jira/browse/TUBEMQ-240) | add status command for broker/master script |  Major |
|[TUBEMQ-241](https://issues.apache.org/jira/browse/TUBEMQ-241) | add helm chart for tubemq |  Major |
|[TUBEMQ-242](https://issues.apache.org/jira/browse/TUBEMQ-242) | Support Table interface for TubeMQ flink connector |  Major |
|[TUBEMQ-244](https://issues.apache.org/jira/browse/TUBEMQ-244) | tubemq web support access using proxy IP |  Major |
|[TUBEMQ-246](https://issues.apache.org/jira/browse/TUBEMQ-246) | support register broker using hostname |  Major |
|[TUBEMQ-295](https://issues.apache.org/jira/browse/TUBEMQ-295) | Modify CHANGES.md to add 0.5.0 version release modification |  Major |
|[TUBEMQ-299](https://issues.apache.org/jira/browse/TUBEMQ-299) | Fix RAT check warnning |  Major |
|[TUBEMQ-300](https://issues.apache.org/jira/browse/TUBEMQ-300) | Update LICENSE |  Major |



## Release 0.3.0-incubating - Released (as of 2020-06-08)

### NEW FEATURES:

| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[TUBEMQ-42](https://issues.apache.org/jira/browse/TUBEMQ-42) | Add peer information about message received	Major	New Feature |  Major|

### IMPROVEMENTS:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [TUBEMQ-16](https://issues.apache.org/jira/browse/TUBEMQ-16) |Correct BdbStoreService#isPrimaryNodeActived to BdbStoreService#isPrimaryNodeActive|	Low|
| [TUBEMQ-18](https://issues.apache.org/jira/browse/TUBEMQ-18) |Correct TMaster#idGenerater to TMaster#idGenerator|	Low|
| [TUBEMQ-19](https://issues.apache.org/jira/browse/TUBEMQ-19) |Correct parameter names to fit in camel case|	Low|
| [TUBEMQ-20](https://issues.apache.org/jira/browse/TUBEMQ-20) |Correct DefaultLoadBalancer#balance parameter	| Low|
| [TUBEMQ-21](https://issues.apache.org/jira/browse/TUBEMQ-21) |Change version number from x.y-SNAPSHOT to x.y.z-incubating-SNAPSHOT|	Normal|
| [TUBEMQ-22](https://issues.apache.org/jira/browse/TUBEMQ-22) |Correct ClientSubInfo#getTopicProcesser -> ClientSubInfo#getTopicProcessor|	Low|
| [TUBEMQ-23](https://issues.apache.org/jira/browse/TUBEMQ-23) |Improve project README content introduction|	Major|
| [TUBEMQ-24](https://issues.apache.org/jira/browse/TUBEMQ-24) |Add NOTICE and adjust LICENSE	| Major|
| [TUBEMQ-26](https://issues.apache.org/jira/browse/TUBEMQ-26) |correct spelling (difftime-> diffTime)	|Low|
| [TUBEMQ-27](https://issues.apache.org/jira/browse/TUBEMQ-27) |replace StringBuffer with StringBuilder |	Major|
| [TUBEMQ-28](https://issues.apache.org/jira/browse/TUBEMQ-28) |ignore path error	|Major|
| [TUBEMQ-29](https://issues.apache.org/jira/browse/TUBEMQ-29) |Change the package name to org.apache.tubemq.""	|Major|
| [TUBEMQ-33](https://issues.apache.org/jira/browse/TUBEMQ-33) |refactor enum implement from annoymouse inner class	| Major|
| [TUBEMQ-38](https://issues.apache.org/jira/browse/TUBEMQ-38) |Add Broker's running status check	| Major||
| [TUBEMQ-39](https://issues.apache.org/jira/browse/TUBEMQ-39) |Optimize the loadMessageStores() logic	| Nor|mal|
| [TUBEMQ-40](https://issues.apache.org/jira/browse/TUBEMQ-40) |Optimize message disk store classes's logic	| Major|
| [TUBEMQ-43](https://issues.apache.org/jira/browse/TUBEMQ-43) |Add DeletePolicy's value check	| Major|
| [TUBEMQ-44](https://issues.apache.org/jira/browse/TUBEMQ-44) |Remove unnecessary synchronized definition of shutdown () function	| Normal|
| [TUBEMQ-49](https://issues.apache.org/jira/browse/TUBEMQ-49) |setTimeoutTime change to updTimeoutTime	| Major|
| [TUBEMQ-50](https://issues.apache.org/jira/browse/TUBEMQ-50) |Replace fastjson to gson	| Major|
| [TUBEMQ-7](https://issues.apache.org/jira/browse/TUBEMQ-7) | Using StringBuilder instead of StringBuffer in BaseResult	| Low|
| [TUBEMQ-9](https://issues.apache.org/jira/browse/TUBEMQ-9) | Remove some unnecessary code	| Minor |

### BUG FIXES:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
| [TUBEMQ-10](https://issues.apache.org/jira/browse/TUBEMQ-10) |Fix Javadoc error|Low|
| [TUBEMQ-14](https://issues.apache.org/jira/browse/TUBEMQ-14) |Some compilation errors|Major|
| [TUBEMQ-15](https://issues.apache.org/jira/browse/TUBEMQ-15) |Correct typo in http_access_API_definition.md|Low|
| [TUBEMQ-32](https://issues.apache.org/jira/browse/TUBEMQ-32) |File path not match with package name in tubemq-client module|Major|
| [TUBEMQ-35](https://issues.apache.org/jira/browse/TUBEMQ-35) |check illegal package's field value|Normal|
| [TUBEMQ-36](https://issues.apache.org/jira/browse/TUBEMQ-36) |Remove unnecessary removefirst() function printing|Normal|
| [TUBEMQ-37](https://issues.apache.org/jira/browse/TUBEMQ-37) |Offset is set to 0 when Broker goes offline|Major|
| [TUBEMQ-45](https://issues.apache.org/jira/browse/TUBEMQ-45) |Check groupName with checkHostName function|Major|
| [TUBEMQ-48](https://issues.apache.org/jira/browse/TUBEMQ-48) |No timeout when setting consumer timeout|Major|
| [TUBEMQ-59](https://issues.apache.org/jira/browse/TUBEMQ-59) |Null pointer exception is thrown while constructing ConsumerConfig with MasterInfo|Normal|
| [TUBEMQ-62](https://issues.apache.org/jira/browse/TUBEMQ-62) |consumed and set consumerConfig.setConsumeModel (0) for the first time|Major|
| [TUBEMQ-66](https://issues.apache.org/jira/browse/TUBEMQ-66) |TubeSingleSessionFactory shutdown bug|Normal|
| [TUBEMQ-85](https://issues.apache.org/jira/browse/TUBEMQ-85) |There is NPE when creating PullConsumer with TubeSingleSessionFactory|Major|
| [TUBEMQ-88](https://issues.apache.org/jira/browse/TUBEMQ-88) |Broker does not take effect after the deletePolicy value is changed|Major|
| [TUBEMQ-149](https://issues.apache.org/jira/browse/TUBEMQ-149) |Some of the consumers stop consuming their corresponding partitions and never release the partition to others|Major|
| [TUBEMQ-153](https://issues.apache.org/jira/browse/TUBEMQ-153) |update copyright notices year to 2020|  Major |
| [TUBEMQ-165](https://issues.apache.org/jira/browse/TUBEMQ-165) |Remove unnecessary fiiles|  Major |

### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[TUBEMQ-12](https://issues.apache.org/jira/browse/TUBEMQ-12)  |Change to use Apache License V2   |  Major |

### SUB-TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[TUBEMQ-130](https://issues.apache.org/jira/browse/TUBEMQ-130) |Generate CHANGES.md and DISCLAIMER-WIP   |  Major |
|[TUBEMQ-133](https://issues.apache.org/jira/browse/TUBEMQ-133) |Add Apache parent pom |  Major |
|[TUBEMQ-134](https://issues.apache.org/jira/browse/TUBEMQ-134) |add maven-source-plugin for generate source jar|  Major |
|[TUBEMQ-135](https://issues.apache.org/jira/browse/TUBEMQ-135) |Refactoring all pom.xml|  Major |
|[TUBEMQ-136](https://issues.apache.org/jira/browse/TUBEMQ-136) |Add LICENSE/NOTICE/DISCLAIMER-WIP to binary package|  Major |
