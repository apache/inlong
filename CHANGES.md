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

## Release 0.3.0-incubating - Unreleased (as of 2020-05-19)

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

### TASK:
| JIRA | Summary | Priority |
|:---- |:---- | :--- |
|[TUBEMQ-12](https://issues.apache.org/jira/browse/TUBEMQ-12)  |Change to use Apache License V2   |  Major |
|[TUBEMQ-130](https://issues.apache.org/jira/browse/TUBEMQ-130)  |Generate CHANGES.md and DISCLAIMER-WIP   |  Major |
