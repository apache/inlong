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


## Release 0.5.0-incubating - Unreleased (as of 2020-07-22)

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
