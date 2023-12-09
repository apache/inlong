
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

# Release InLong 1.10.0 - Released (as of 2023-12-05)
### Agent
|                            ISSUE                            | Summary                                                                                                  |
|:-----------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------|
| [INLONG-9089](https://github.com/apache/inlong/issues/9089) | [Improve][Agent] Add enums for task and instance                                                         |
| [INLONG-9091](https://github.com/apache/inlong/issues/9091) | [Improve][Agent] Add offset profile                                                                      |
| [INLONG-9094](https://github.com/apache/inlong/issues/9094) | [Umbrella][Agent] Reconfiguration of task management                                                     |
| [INLONG-9102](https://github.com/apache/inlong/issues/9102) | [Improve][Agent] Add file utils                                                                          |
| [INLONG-9112](https://github.com/apache/inlong/issues/9112) | [Improve][Agent] Add task and instance profile                                                           |
| [INLONG-9117](https://github.com/apache/inlong/issues/9117) | [Improve][Agent] Rewrite class RocksDbImp to enable it to be constructed with a child path               |
| [INLONG-9120](https://github.com/apache/inlong/issues/9120) | [Improve][Agent] Add offset db to store the offset data                                                  |
| [INLONG-9122](https://github.com/apache/inlong/issues/9122) | [Improve][Agent] Add task and instance action                                                            |
| [INLONG-9124](https://github.com/apache/inlong/issues/9124) | [Improve][Agent] Add task and instance db                                                                |
| [INLONG-9125](https://github.com/apache/inlong/issues/9125) | [Improve][Agent] Add offset manager                                                                      |
| [INLONG-9132](https://github.com/apache/inlong/issues/9132) | [Improve][Agent] Add file used message cache                                                             |
| [INLONG-9134](https://github.com/apache/inlong/issues/9134) | [Improve][Agent] Add file related utils                                                                  |
| [INLONG-9136](https://github.com/apache/inlong/issues/9136) | [Improve][Agent] Add instance manager                                                                    |
| [INLONG-9138](https://github.com/apache/inlong/issues/9138) | [Improve][Agent] Add task manager                                                                        |
| [INLONG-9143](https://github.com/apache/inlong/issues/9143) | [Improve][Agent] Add log file collect task                                                               |
| [INLONG-9149](https://github.com/apache/inlong/issues/9149) | [Improve][Agent] Add sender manager for file collect                                                     |
| [INLONG-9151](https://github.com/apache/inlong/issues/9151) | [Improve][Agent] Add log file source and source related modification                                     |
| [INLONG-9155](https://github.com/apache/inlong/issues/9155) | [Improve][Agent] Add file used proxy                                                                     |
| [INLONG-9159](https://github.com/apache/inlong/issues/9159) | [Improve][Agent] Add file instance                                                                       |
| [INLONG-9161](https://github.com/apache/inlong/issues/9161) | [Improve][Agent] Modify left sink                                                                        |
| [INLONG-9163](https://github.com/apache/inlong/issues/9163) | [Improve][Agent] Delete trigger related file                                                             |
| [INLONG-9165](https://github.com/apache/inlong/issues/9165) | [Improve][Agent] Delete job related file                                                                 |
| [INLONG-9182](https://github.com/apache/inlong/issues/9182) | [Improve][Agent] Delete useless code                                                                     |
| [INLONG-9187](https://github.com/apache/inlong/issues/9187) | [Improve][Agent] Delete useless memory manager                                                           |
| [INLONG-9190](https://github.com/apache/inlong/issues/9190) | [Bug][Agent] Log file source clear buffer queue does not take effect                                     |
| [INLONG-9194](https://github.com/apache/inlong/issues/9194) | [Bug][Agent] Calc time offset failed if the param is 0                                                   |
| [INLONG-9200](https://github.com/apache/inlong/issues/9200) | [Bug][Agent] Duplicate file collect instance                                                             |
| [INLONG-9207](https://github.com/apache/inlong/issues/9207) | [Bug][Agent] Task manager stuck                                                                          |
| [INLONG-9214](https://github.com/apache/inlong/issues/9214) | [Improve][Agent] Limit max file count to collect once                                                    |
| [INLONG-9215](https://github.com/apache/inlong/issues/9215) | [Improve][Agent] Add predefine fields                                                                    |
| [INLONG-9233](https://github.com/apache/inlong/issues/9233) | [Bug][Agent] Fix bug: source, proxy, sender get stuck                                                    |
| [INLONG-9237](https://github.com/apache/inlong/issues/9237) | [Improve][Agent] Move addictive fields to package attributes                                             |
| [INLONG-9241](https://github.com/apache/inlong/issues/9241) | [Improve][Agent] Print task and instance detail every ten seconds                                        |
| [INLONG-9244](https://github.com/apache/inlong/issues/9244) | [Bug][Agent] Fix bug: miss file from next data time                                                      |
| [INLONG-9253](https://github.com/apache/inlong/issues/9253) | [Bug][Agent] Get byte position of file by line count offset failed                                       |
| [INLONG-9263](https://github.com/apache/inlong/issues/9263) | [Improve][Agent] Print the statistics of task and instance not detail                                    |
| [INLONG-9265](https://github.com/apache/inlong/issues/9265) | [Improve][Agent] Add audit of agent send success                                                         |
| [INLONG-9267](https://github.com/apache/inlong/issues/9267) | [Bug][Agent] Data loss when there are many files to read once                                            |
| [INLONG-9284](https://github.com/apache/inlong/issues/9284) | [Improve][Agent] Report audit by data time not real time                                                 |
| [INLONG-9286](https://github.com/apache/inlong/issues/9286) | [Improve][Agent] Adjust the time offset calculation function                                             |
| [INLONG-9289](https://github.com/apache/inlong/issues/9289) | [Improve][Agent] Improve the completion judgment logic of collecting instances                           |
| [INLONG-9300](https://github.com/apache/inlong/issues/9300) | [Improve][Agent] Divide data time into source time and sink time                                         |
| [INLONG-9308](https://github.com/apache/inlong/issues/9308) | [Improve][Agent] The sink end of the file instance supports sending data with different streamIds        |
| [INLONG-9310](https://github.com/apache/inlong/issues/9310) | [Improve][Agent] Add extended handler in file source                                                     |
| [INLONG-9312](https://github.com/apache/inlong/issues/9312) | [Improve][Agent] Add data content style                                                                  |
| [INLONG-9315](https://github.com/apache/inlong/issues/9315) | [Improve][Agent] Convert data time from source time zone to sink time zone.                              |
| [INLONG-9317](https://github.com/apache/inlong/issues/9317) | [Improve][Agent] Print basic info of hearbeat                                                            |
| [INLONG-9335](https://github.com/apache/inlong/issues/9335) | [Improve][Agent] Bring cycle parameters when creating an instance                                        |
| [INLONG-9338](https://github.com/apache/inlong/issues/9338) | [Improve][Agent] Real time file collection uses the current time as the data time                        |
| [INLONG-9347](https://github.com/apache/inlong/issues/9347) | [Improve][Agent] Check task profile before save into db                                                  |
| [INLONG-9364](https://github.com/apache/inlong/issues/9364) | [Improve][Agent] Remove expired instance from db                                                         |
| [INLONG-9366](https://github.com/apache/inlong/issues/9366) | [Improve][Agent] Remove useless offset record                                                            |
| [INLONG-9369](https://github.com/apache/inlong/issues/9369) | [Improve][Agent] Increase sending failure audit and real-time audit                                      |
| [INLONG-9375](https://github.com/apache/inlong/issues/9375) | [Improve][Agent] Modify the agent's real-time audit id to prevent duplication                            |
| [INLONG-9390](https://github.com/apache/inlong/issues/9390) | [Improve][Agent] Collect supplementary data in chronological order                                       |
| [INLONG-9397](https://github.com/apache/inlong/issues/9397) | [Improve][Agent] Do not directly delete the instance records of the local db when stopping the instances |                                
                                                                                                                                                                                                
### Audit
|                            ISSUE                            | Summary                                                                 |
|:-----------------------------------------------------------:|:------------------------------------------------------------------------|
| [INLONG-9225](https://github.com/apache/inlong/issues/9225) | [Improve][Audit] Automatically create audit topic after service startup |

### Dashboard
|                            ISSUE                            | Summary                                                                                   |
|:-----------------------------------------------------------:|:------------------------------------------------------------------------------------------|
| [INLONG-8983](https://github.com/apache/inlong/issues/8983) | [Improve][Dashboard] Log component query optimization                                     |
| [INLONG-9016](https://github.com/apache/inlong/issues/9016) | [Improve][Dashboard] Change the wrapWithInlongMsg to wrapType in data stream              |
| [INLONG-9053](https://github.com/apache/inlong/issues/9053) | [Improve][Dashboard] Query list shows creators and modifiers                              |
| [INLONG-9055](https://github.com/apache/inlong/issues/9055) | [Improve][Dashboard] The approval management list displays the name of the consumer group |
| [INLONG-9107](https://github.com/apache/inlong/issues/9107) | [Improve][Dashboard] Navigation widget optimization                                       |
| [INLONG-9114](https://github.com/apache/inlong/issues/9114) | [Improve][Dashboard] Stream log query parameter optimization                              |
| [INLONG-9185](https://github.com/apache/inlong/issues/9185) | [Feature][Dashboard] Cluster management supports sort_cls, sort_es, sort_pulsar types     |
| [INLONG-9196](https://github.com/apache/inlong/issues/9196) | [Improve][Dashboard] Navigation widget user manual optimization                           |
| [INLONG-9219](https://github.com/apache/inlong/issues/9219) | [Improve][Dashboard] Stream advanced options parameter optimization                       |
| [INLONG-9229](https://github.com/apache/inlong/issues/9229) | [Improve][Dashboard] Data access transmission delay optimization                          |
| [INLONG-9256](https://github.com/apache/inlong/issues/9256) | [Improve][Dashboard] Data synchronization iceberg sink supports append Mode               |
| [INLONG-9260](https://github.com/apache/inlong/issues/9260) | [Improve][Dashboard] Support batch import in Synchronize dashboard and detail page        |
| [INLONG-9302](https://github.com/apache/inlong/issues/9302) | [Improve][Dashboard] Elasticsearch sink optimization                                      |
| [INLONG-9322](https://github.com/apache/inlong/issues/9322) | [Improve][Dashboard] Sink field mapping title format is unified                           |
| [INLONG-9354](https://github.com/apache/inlong/issues/9354) | [Improve][Dashboard] Data access File sources optimization                                |
| [INLONG-9368](https://github.com/apache/inlong/issues/9368) | [Improve][Dashboard] Creation time or other time display in an easily readable format     |
| [INLONG-9383](https://github.com/apache/inlong/issues/9383) | [Improve][Dashboard] Resource details display cluster label information                   |
| [INLONG-9385](https://github.com/apache/inlong/issues/9385) | [Improve][Dashboard] Create a new cluster and remove the agent and dataproxy types        |
| [INLONG-9427](https://github.com/apache/inlong/issues/9427) | [Improve][Dashboard] Data access supports viewing operation logs                          |
                                                
### Manager
|                            ISSUE                            | Summary                                                                                                                                       |
|:-----------------------------------------------------------:|:----------------------------------------------------------------------------------------------------------------------------------------------|
| [INLONG-8958](https://github.com/apache/inlong/issues/8958) | [Bug][Manager]Create cls topic fail                                                                                                           |
| [INLONG-8960](https://github.com/apache/inlong/issues/8960) | [Feature][Manager] Automatically assign sort cluster after creating stream sinks                                                              |
| [INLONG-8964](https://github.com/apache/inlong/issues/8964) | [Feature][Manager] Support Sortstandalone cluster management                                                                                  |
| [INLONG-8975](https://github.com/apache/inlong/issues/8975) | [Improve][Manager] The manager issues an audit IDs based on the group mode                                                                    |
| [INLONG-8986](https://github.com/apache/inlong/issues/8986) | [Bug][Manager] Sort standalone get params from manager error                                                                                  |
| [INLONG-8988](https://github.com/apache/inlong/issues/8988) | [Improve][Manager] Supports multiple wrap types for message body                                                                              |
| [INLONG-8990](https://github.com/apache/inlong/issues/8990) | [Improve][Manager][DataProxy][SDK] Rename DataProxyMsgEncType to Message WrapType                                                             |
| [INLONG-8992](https://github.com/apache/inlong/issues/8992) | [Bug][Manager] NPE caused by not using transform                                                                                              |
| [INLONG-8995](https://github.com/apache/inlong/issues/8995) | [Improve][Manager] Add an interface for querying used task information for agent                                                              |
| [INLONG-9003](https://github.com/apache/inlong/issues/9003) | [Feature][DataProxy][Manager] Support extend fields                                                                                           |
| [INLONG-9017](https://github.com/apache/inlong/issues/9017) | [Improve][Manager] Add stop interface in stream source API                                                                                    |
| [INLONG-9023](https://github.com/apache/inlong/issues/9023) | [Bug][Manager] Failed to stop job task                                                                                                        |
| [INLONG-9027](https://github.com/apache/inlong/issues/9027) | [Bug][Manager] Fix SQL error in ClusterSetMapper.selectInlongStreamId                                                                         |
| [INLONG-9036](https://github.com/apache/inlong/issues/9036) | [Improve][Manager] Support create tables for dataSync                                                                                         |
| [INLONG-9041](https://github.com/apache/inlong/issues/9041) | [Improve][Manager] Support saving schema information when saving iceberg source                                                               |
| [INLONG-9051](https://github.com/apache/inlong/issues/9051) | [Improve][Manager] Add consumer group in the approval form                                                                                    |
| [INLONG-9060](https://github.com/apache/inlong/issues/9060) | [Bug][Manager] Manager return wrong sink configuration to sort standalone                                                                     |
| [INLONG-9062](https://github.com/apache/inlong/issues/9062) | [Improve][Manager] Add default values for stream related interface parameters                                                                 |
| [INLONG-9066](https://github.com/apache/inlong/issues/9066) | [Improve][Manager] Renaming group related states                                                                                              |
| [INLONG-9069](https://github.com/apache/inlong/issues/9069) | [Improve][Manager] Filter out invalid configs when organize SortStandalone configuration                                                      |
| [INLONG-9070](https://github.com/apache/inlong/issues/9070) | [Improve][Manager] Add MessageWrapType.forType method for MessageWrapType                                                                     |
| [INLONG-9073](https://github.com/apache/inlong/issues/9073) | [Improve][Manager] Add getStreamBriefInfo  method for InlongStreamClient                                                                      |
| [INLONG-9092](https://github.com/apache/inlong/issues/9092) | [Improve][Manager] Tube supports inlong-msg format                                                                                            |
| [INLONG-9098](https://github.com/apache/inlong/issues/9098) | [Improve][Manager] Support to save additional info for the Iceberg field                                                                      |
| [INLONG-9106](https://github.com/apache/inlong/issues/9106) | [Improve][Manager] Support configuring multiple job tasks under a group                                                                       |
| [INLONG-9109](https://github.com/apache/inlong/issues/9109) | [Bug][Manager] Incorrect URL address for getbrief mapping in streamApi                                                                        |
| [INLONG-9147](https://github.com/apache/inlong/issues/9147) | [Improve][Manager] The group details are displayed with the tag of the cluster to which the group belongs                                     |
| [INLONG-9192](https://github.com/apache/inlong/issues/9192) | [Feature][Manager] Flat Sort Cluster types                                                                                                    |
| [INLONG-9203](https://github.com/apache/inlong/issues/9203) | [Bug][Manager][Sort] Failed to use Iceberg sink                                                                                               |
| [INLONG-9205](https://github.com/apache/inlong/issues/9205) | [Bug][Manager] Failed to use Hudi sink                                                                                                        |
| [INLONG-9209](https://github.com/apache/inlong/issues/9209) | [Improve][Manager] Support configuring predefined fields and issuing agents                                                                   |
| [INLONG-9211](https://github.com/apache/inlong/issues/9211) | [Bug][Manager] Redis cannot get the clusterMode from sink                                                                                     |
| [INLONG-9240](https://github.com/apache/inlong/issues/9240) | [Improve][Sort][Manager] Add options for Iceberg connector                                                                                    |
| [INLONG-9248](https://github.com/apache/inlong/issues/9248) | [Improve][Manager] Supports configuring builtIn fields for tube  source and pulsar source                                                     |
| [INLONG-9250](https://github.com/apache/inlong/issues/9250) | [Improve][Manager] Add auditId for tube, pulsar, and mysql                                                                                    |
| [INLONG-9259](https://github.com/apache/inlong/issues/9259) | [Feature][Manager] Optimize Elasticsearch sink and datanode                                                                                   |
| [INLONG-9269](https://github.com/apache/inlong/issues/9269) | [Bug][Manager] Get SortClusterConfig is empty,when sink params include non-string type                                                        |
| [INLONG-9280](https://github.com/apache/inlong/issues/9280) | [Feature][Manager] Support different size of extended fields of InlongStream                                                                  |
| [INLONG-9285](https://github.com/apache/inlong/issues/9285) | [Bug][Manager] When creating KafkaSource, the autoOffsetReset param cannot be empty                                                           |
| [INLONG-9297](https://github.com/apache/inlong/issues/9297) | [Feature][Manager] Support configuring multiple sink types of tasks under a single stream                                                     |
| [INLONG-9303](https://github.com/apache/inlong/issues/9303) | [Feature][Manager]Support Tube MQ sink                                                                                                        |
| [INLONG-9314](https://github.com/apache/inlong/issues/9314) | [Feature][Manager] Support cluster switch for InlongGroup                                                                                     |
| [INLONG-9318](https://github.com/apache/inlong/issues/9318) | [Improve][Manager] ManagerClient supports pulling clusters based on tenant roles                                                              |
| [INLONG-9328](https://github.com/apache/inlong/issues/9328) | [Improve][Manager] Add parameters validation for the updateAuditSource method                                                                 |
| [INLONG-9330](https://github.com/apache/inlong/issues/9330) | [Improve][Manager] Add encoding check to the StarRocks JDBC URL                                                                               |
| [INLONG-9337](https://github.com/apache/inlong/issues/9337) | [Improve][Manager] Support querying operation records                                                                                         |
| [INLONG-9343](https://github.com/apache/inlong/issues/9343) | [Improve][Manager] Support configuring timeZone related parameters for fileSource                                                             |
| [INLONG-9351](https://github.com/apache/inlong/issues/9351) | [Improve][Manager] Support querying audit data size                                                                                           |
| [INLONG-9358](https://github.com/apache/inlong/issues/9358) | [Bug][Manager] The creation time of the information in the database differs from the creation time in the returned information by eight hours |
| [INLONG-9362](https://github.com/apache/inlong/issues/9362) | [Improve][Manager] Support the configuration of parameters related to the migration of the entire Iceberg database                            |
| [INLONG-9373](https://github.com/apache/inlong/issues/9373) | [Bug][Manager] Failed to create namespace                                                                                                     |
| [INLONG-9400](https://github.com/apache/inlong/issues/9400) | [Bug][Manager] Error obtaining sort task type                                                                                                 |                                                                                               
| [INLONG-9433](https://github.com/apache/inlong/issues/9433) | [Bug][Manager] NPE exception encountered while querying audit information                                                                     |
| [INLONG-9444](https://github.com/apache/inlong/issues/9444) | [Bug][Manager] ES sink unsuccessful flow status configuration successful                                                                      |
| [INLONG-9447](https://github.com/apache/inlong/issues/9447) | [Bug][Manager] Suspend group failed                                                                                                           |
                                                         
### SDK
|                            ISSUE                            | Summary                                                                                                  |
|:-----------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------|
| [INLONG-8990](https://github.com/apache/inlong/issues/8990) | [Improve][Manager][DataProxy][SDK] Rename DataProxyMsgEncType to Message WrapType                        |
| [INLONG-9007](https://github.com/apache/inlong/issues/9007) | [Improve][SDK] ClientList out of bounds                                                                  |
| [INLONG-9044](https://github.com/apache/inlong/issues/9044) | [Improve][SDK] Add server response log to facilitate troubleshooting                                     |
| [INLONG-9058](https://github.com/apache/inlong/issues/9058) | [Improve][SDK] Limit the number of inlong-groupid and inlong-streamid of a single SDK instance           |
| [INLONG-9079](https://github.com/apache/inlong/issues/9079) | [Improve][SDK] Shaded some dependency to reduce conflicts with other sdk                                 |
| [INLONG-9083](https://github.com/apache/inlong/issues/9083) | [Improve][SDK] Bump up Golang SDK x/net modules                                                          |
| [INLONG-9167](https://github.com/apache/inlong/issues/9167) | [Improve][SDK] Use UUID as the batch ID instead of snowflake ID for DataProxy Golang SDK                 |
| [INLONG-9170](https://github.com/apache/inlong/issues/9170) | [Improve][SDK] Use pointer instead of object for chans in Golang SDK                                     |
| [INLONG-9172](https://github.com/apache/inlong/issues/9172) | [Improve][SDK] comment/delete some debug log in some frequently called methods for Golang SDK            |
| [INLONG-9174](https://github.com/apache/inlong/issues/9174) | [Improve][SDK] Improve response attr parsing in Golang SDK                                               |
| [INLONG-9176](https://github.com/apache/inlong/issues/9176) | [Improve][SDK] Fail fast when worker is unavailable in Golang SDK                                        |
| [INLONG-9178](https://github.com/apache/inlong/issues/9178) | [Improve][SDK] Update the default values of the config options of Golang SDK                             |
| [INLONG-9180](https://github.com/apache/inlong/issues/9180) | [Improve][SDK] Cache up batchReq.dataReqs                                                                |
| [INLONG-9184](https://github.com/apache/inlong/issues/9184) | [Improve][SDK] Update README.md in Golang SDK                                                            |
| [INLONG-9213](https://github.com/apache/inlong/issues/9213) | [Improve][SDK] Support isolation by inlong groupid                                                       |
| [INLONG-9228](https://github.com/apache/inlong/issues/9228) | [Improve][SDK] CPP SDK supports dynamic load balancing                                                   |
| [INLONG-9277](https://github.com/apache/inlong/issues/9277) | [Feature][SDK] Optimize multi-region nearby access                                                       |
| [INLONG-9293](https://github.com/apache/inlong/issues/9293) | [Improve][SDK] Optimize the problem that the more inlong grouids there are, the more memory is consumed. |
| [INLONG-9307](https://github.com/apache/inlong/issues/9307) | [Improve][SDK] Improve DataProxy SDK code readability                                                    |
| [INLONG-9320](https://github.com/apache/inlong/issues/9320) | [Improve][SDK] Support local disaster recovery manager configuration                                     |
| [INLONG-9324](https://github.com/apache/inlong/issues/9324) | [Improve][SDK] Supports automatic creation of custom log directories                                     |
| [INLONG-9341](https://github.com/apache/inlong/issues/9341) | [Improve][SDK] Optimize obtaining local IP information                                                   |
| [INLONG-9345](https://github.com/apache/inlong/issues/9345) | [Bug][SDK] DataProxy SDK throws java.lang.NoClassDefFoundErrorException                                  |
| [INLONG-9355](https://github.com/apache/inlong/issues/9355) | [Improve][SDK] Optimize resource isolation for CPP SDK                                                   |
| [INLONG-9378](https://github.com/apache/inlong/issues/9378) | [Improve][SDK] Optimize proxy configuration update                                                       |                                                             

### Sort
|                            ISSUE                            | Summary                                                                                       |
|:-----------------------------------------------------------:|:----------------------------------------------------------------------------------------------|
| [INLONG-8959](https://github.com/apache/inlong/issues/8959) | [Feature][Sort]Add mongodb connector on flink 1.15                                            |
| [INLONG-8973](https://github.com/apache/inlong/issues/8973) | [Bug][Sort] Fix PRIMARY KEY desc error in redis readme.                                       |
| [INLONG-8979](https://github.com/apache/inlong/issues/8979) | [Bug][Sort] Wrong desc about redis-conenctor in flink 1.13 module about command option        |
| [INLONG-8982](https://github.com/apache/inlong/issues/8982) | [Feature][Sort] Iceberg sink on flink 1.15                                                    |
| [INLONG-8997](https://github.com/apache/inlong/issues/8997) | [Improve][Sort] Keep the logic the same with flink v1.15 mysql in flink v1.13 end to end test |
| [INLONG-8998](https://github.com/apache/inlong/issues/8998) | [Improve][Sort] Add sqlserver connector of flink 1.15                                         |
| [INLONG-8999](https://github.com/apache/inlong/issues/8999) | [Feature][Sort] Add sqlserver connector of flink 1.15                                         |
| [INLONG-9009](https://github.com/apache/inlong/issues/9009) | [Feature][Sort] Add HBase connector on flink 1.15                                             |
| [INLONG-9038](https://github.com/apache/inlong/issues/9038) | [Bug][Sort] Fail to parse InlongMessage when exporting Pulsar                                 |
| [INLONG-9075](https://github.com/apache/inlong/issues/9075) | [Improve][Sort] TubeMQSource support InlongMsg format                                         |
| [INLONG-9077](https://github.com/apache/inlong/issues/9077) | [Bug][Sort] TubeMQ connector fail to subscribe streamId                                       |
| [INLONG-9084](https://github.com/apache/inlong/issues/9084) | [Bug][Sort] Fail to parse InlongGroupId when report Inlong Audit                              |
| [INLONG-9087](https://github.com/apache/inlong/issues/9087) | [Improve][Sort] TubeMQ Connector use latest offset mode                                       |
| [INLONG-9128](https://github.com/apache/inlong/issues/9128) | [Bug][Sort] Failed to init TubeMQ source with InlongMsg type message                          |
| [INLONG-9203](https://github.com/apache/inlong/issues/9203) | [Bug][Manager][Sort] Failed to use Iceberg sink                                               |
| [INLONG-9223](https://github.com/apache/inlong/issues/9223) | [Feature][Sort] TubeMQ souce support InlongAudit                                              |
| [INLONG-9231](https://github.com/apache/inlong/issues/9231) | [Bug][Sort] Find no audit time field when the filed is in upper case                          |
| [INLONG-9240](https://github.com/apache/inlong/issues/9240) | [Improve][Sort][Manager] Add options for Iceberg connector                                    |
| [INLONG-9246](https://github.com/apache/inlong/issues/9246) | [Improve][Sort] Pulsar source support audit when the deserialized type is not InlongMsg       |
| [INLONG-9247](https://github.com/apache/inlong/issues/9247) | [Improve][Sort] TubeMQ source support audit when the deserialized type is not InlongMsg       |
| [INLONG-9273](https://github.com/apache/inlong/issues/9273) | [Bug][Sort] IcebergSingleFileCommiter will throw exception for error code                     |
| [INLONG-9333](https://github.com/apache/inlong/issues/9333) | [Bug][Sort] Related problem with attribute exceptions when creating 'hudiSink'                |
| [INLONG-9371](https://github.com/apache/inlong/issues/9371) | [Bug][Sort] elasticsearch sort shade relocation error                                         |
| [INLONG-9377](https://github.com/apache/inlong/issues/9377) | [Bug][Sort] Failed to init iceberg sink with upsert mode                                      |
| [INLONG-9380](https://github.com/apache/inlong/issues/9380) | [Bug][Sort] Audit lost when stop job immediately after checkpoint                             |
| [INLONG-9388](https://github.com/apache/inlong/issues/9388) | [Improve][Sort] Updated version of embedded-redis in sort-connector-redis                     |
| [INLONG-9392](https://github.com/apache/inlong/issues/9392) | [Bug][Sort] MySqlContainer class getJdbcUrl() method splicing querystring error               |
| [INLONG-9394](https://github.com/apache/inlong/issues/9394) | [Improve][Sort] Incorrect use of maven plugin on integration test among sort-end-to-end-tests |                                                            
| [INLONG-9417](https://github.com/apache/inlong/issues/9417) | [Bug][Sort] Missing MySQL CDC connector for Flink 1.15                                        |

### Other
|                            ISSUE                            | Summary                                                                                                           |
|:-----------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------|
| [INLONG-8961](https://github.com/apache/inlong/issues/8961) | [Improve] Server-side request forgery attack prevention in some classes                                           |
| [INLONG-8967](https://github.com/apache/inlong/issues/8967) | [Feature] Add Mysql connector on flink 1.15                                                                       |
| [INLONG-8971](https://github.com/apache/inlong/issues/8971) | [Improve][manager] Missing audit id for MQ                                                                        |
| [INLONG-8977](https://github.com/apache/inlong/issues/8977) | [Feature] Add tube source connector on flink 1.15                                                                 |
| [INLONG-8994](https://github.com/apache/inlong/issues/8994) | [Feature] Add hudi connector on flink 1.15                                                                        |
| [INLONG-9013](https://github.com/apache/inlong/issues/9013) | [Improve][DataProxy] Inconsistent annotations                                                                     |
| [INLONG-9025](https://github.com/apache/inlong/issues/9025) | [Improve][Build] Avoid deploying the distribution packages to repositories                                        |
| [INLONG-9034](https://github.com/apache/inlong/issues/9034) | [Improve] Fix sort redis test with incorrect use of sleep                                                         |
| [INLONG-9046](https://github.com/apache/inlong/issues/9046) | [Improve][CVE] snappy-java's missing upper bound check on chunk length can lead to Denial of Service (DoS) impact |
| [INLONG-9064](https://github.com/apache/inlong/issues/9064) | [Feature] Add Audit report for Pulsar connector in flink 1.15                                                     |
| [INLONG-9081](https://github.com/apache/inlong/issues/9081) | [Bug] Pulsar connector in flink 1.15 should running in exclusive mode                                             |
| [INLONG-9095](https://github.com/apache/inlong/issues/9095) | [Feature] Support inlong-msg in pulsar flink 1.15 connector                                                       |
| [INLONG-9104](https://github.com/apache/inlong/issues/9104) | [Improve][ASF] Add a protected branches strategy for the master branch                                            |
| [INLONG-9202](https://github.com/apache/inlong/issues/9202) | [Bug] Fix audit report error when running pulsar -> iceberg in flink1.15                                          |
| [INLONG-9221](https://github.com/apache/inlong/issues/9221) | [Bug]When sink is MySQL, there is no 'DataNode' when first selected                                               |
| [INLONG-9225](https://github.com/apache/inlong/issues/9225) | [Improve][Audit] Automatically create audit topic after service startup                                           |
| [INLONG-9271](https://github.com/apache/inlong/issues/9271) | [Bug] When creating 'StreamField', 'isMetaField' must be initialized                                              |
| [INLONG-9281](https://github.com/apache/inlong/issues/9281) | [Bug] Fix pulsar flink connector cannot support batch messages                                                    |
| [INLONG-9296](https://github.com/apache/inlong/issues/9296) | [Bug] Cluster type 'DATAPROXY' not supported                                                                      |
| [INLONG-9299](https://github.com/apache/inlong/issues/9299) | [Feature] Iceberg support all migrate and auto create table                                                       |
| [INLONG-9359](https://github.com/apache/inlong/issues/9359) | [Bug] Fix iceberg all migrate connector stack overflow error                                                      |
| [INLONG-9384](https://github.com/apache/inlong/issues/9384) | [Bug] Fix audi report loss in pulsar connector                                                                    |                                                                   
