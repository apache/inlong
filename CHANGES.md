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


## Release 1.0.0-incubating - Released (as of 2022-2-1)

### FEATURES:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLLONG-2347](https://github.com/apache/incubator-inlong/issues/2347) | [Feature]Support hive sink in sort-single-tenant |
| [INLLONG-2334](https://github.com/apache/incubator-inlong/issues/2334) | [Feature][inlong-dataproxy]create pulsar client need support config ioThreads |
| [INLLONG-2333](https://github.com/apache/incubator-inlong/issues/2333) | [Feature]Support clickhouse sink in sort-single-tenant |
| [INLLONG-2266](https://github.com/apache/incubator-inlong/issues/2266) | [Feature]Support reporting metrics by audit-sdk in sort | 
| [INLLONG-2250](https://github.com/apache/incubator-inlong/issues/2250) | [Feature][InLong-Sort] Support Kafka sink in InLong-Sort | 
| [INLLONG-2247](https://github.com/apache/incubator-inlong/issues/2247) | Read the consume group offset and store to the specified topic | 
| [INLLONG-2236](https://github.com/apache/incubator-inlong/issues/2236) | [Feature]Support iceberg sink in sort-single-tenant |
| [INLLONG-2232](https://github.com/apache/incubator-inlong/issues/2232) | Add start and end timestamp of segment |
| [INLLONG-2218](https://github.com/apache/incubator-inlong/issues/2218) | [Feature][InLong-DataProxy] Inlong-DataProxy support authentication access pulsar |
| [INLLONG-2217](https://github.com/apache/incubator-inlong/issues/2217) | [Feature][InLong-DataProxy] Add TCP protocol client demo and config doc feature |
| [INLLONG-2216](https://github.com/apache/incubator-inlong/issues/2216) | [Feature][InLong-DataProxy] Add UDP protocol  client demo and config doc |
| [INLLONG-2215](https://github.com/apache/incubator-inlong/issues/2215) | [Feature][InLong-DataProxy] Add http protocol  client demo and config doc |
| [INLLONG-2326](https://github.com/apache/incubator-inlong/issues/2326) | [Feature] Inlong-Sort-Standalone support to sort the events to ElasticSearch cluster. |
| [INLLONG-2322](https://github.com/apache/incubator-inlong/issues/2322) | [Feature][InLong-Sort] Support json format for kafka sink |
| [INLLONG-2301](https://github.com/apache/incubator-inlong/issues/2301) | [Feature]  Support Standalone deployment for InLong |
| [INLLONG-2207](https://github.com/apache/incubator-inlong/issues/2207) | [Feature][InLong-Website] Add component about charts |  
| [INLLONG-2187](https://github.com/apache/incubator-inlong/issues/2187) | [Feature] Website support audit view |                
| [INLLONG-2183](https://github.com/apache/incubator-inlong/issues/2183) | [Feature][InLong-Sort] Bump flink version to 1.13.5 | 
| [INLLONG-2176](https://github.com/apache/incubator-inlong/issues/2176) | Add histogram metric and client-side metric output |  
| [INLLONG-2170](https://github.com/apache/incubator-inlong/issues/2170) | [Feature] add Inlong-Sort-standalone document. |
| [INLLONG-2169](https://github.com/apache/incubator-inlong/issues/2169) | [Feature] [Agent] should provide docs for agent db sql collect |
| [INLLONG-2167](https://github.com/apache/incubator-inlong/issues/2167) | [Feature] [Agent] support db SQL collect |
| [INLLONG-2164](https://github.com/apache/incubator-inlong/issues/2164) | [Feature] Sort-standalone expose metric data using prometheus HttpServer. |
| [INLLONG-2161](https://github.com/apache/incubator-inlong/issues/2161) | [Feature][InLong-Manager] Manager support getClusterConfig  |
| [INLLONG-2138](https://github.com/apache/incubator-inlong/issues/2138) | [Feature] Agent should provide docs for programmers to customize their own source or sink | 
| [INLLONG-2106](https://github.com/apache/incubator-inlong/issues/2106) | [Feature] DataProxy expose metric data using prometheus HttpServer. | 
| [INLLONG-2096](https://github.com/apache/incubator-inlong/issues/2096) | [Feature] DataProxy add InlongGroupId+InlongStreamId metric dimensions in TDSDKSource and TubeSink. |
| [INLLONG-2077](https://github.com/apache/incubator-inlong/issues/2077) | [Feature]sort-sdk change pulsar consume mode from listener to fetch |
| [INLLONG-2076](https://github.com/apache/incubator-inlong/issues/2076) | [Feature] Tube sink of DataProxy support new Message format. |
| [INLLONG-2075](https://github.com/apache/incubator-inlong/issues/2075) | [Feature] SDK Source of DataProxy support new Message format. |
| [INLLONG-2058](https://github.com/apache/incubator-inlong/issues/2058) | [Feature] The metric of Sort-standalone append a dimension(minute level) of event time, supporting audit reconciliation of minute level.  |
| [INLLONG-2056](https://github.com/apache/incubator-inlong/issues/2056) | [Feature]The metric of DataProxy append a dimension(minute level) of event time, supporting audit reconciliation of minute level. |
| [INLLONG-2055](https://github.com/apache/incubator-inlong/issues/2055) | [Feature] [InLong audit] Audit SDK Support real-time report  |
| [INLLONG-2054](https://github.com/apache/incubator-inlong/issues/2054) | [Feature] [InLong audit] Audit SDK Support disaster tolerance |
| [INLLONG-2053](https://github.com/apache/incubator-inlong/issues/2053) | [Feature] [InLong audit] Audit Web Page Display |
| [INLLONG-2051](https://github.com/apache/incubator-inlong/issues/2051) | [Feature] [InLong audit] Add Audit API for Manager |
| [INLLONG-2050](https://github.com/apache/incubator-inlong/issues/2050) | [Feature] [InLong audit] Audit Strore for Elasticsearch |
| [INLLONG-2045](https://github.com/apache/incubator-inlong/issues/2045) | [Feature]sort-sdk support Prometheus monitor |
| [INLLONG-2028](https://github.com/apache/incubator-inlong/issues/2028) | [Feature][CI] Add support for docker build on GitHub Actions |
| [INLLONG-1992](https://github.com/apache/incubator-inlong/issues/1992) | [Feature]sort-flink support configurable loader of getting configuration. |
| [INLLONG-1950](https://github.com/apache/incubator-inlong/issues/1950) | [Feature] DataProxy add supporting to udp protocol for reporting data |
| [INLLONG-1949](https://github.com/apache/incubator-inlong/issues/1949) | [Feature] DataProxy sdk add demo  |
| [INLLONG-1931](https://github.com/apache/incubator-inlong/issues/1931) | [Feature]Inlong-Sort-Standalone-readapi support to consume events from inlong cache clusters(tube) |
| [INLLONG-1895](https://github.com/apache/incubator-inlong/issues/1895) | [Feature]Inlong-Sort-Standalone support to sort the events to Hive cluster. |
| [INLLONG-1894](https://github.com/apache/incubator-inlong/issues/1894) | [Feature]Inlong-Sort-Standalone support JMX metrics listener for pushing. |
| [INLLONG-1892](https://github.com/apache/incubator-inlong/issues/1892) | [Feature]Inlong-Sort-Standalone support to consume events from Pulsar cache clusters. |
| [INLLONG-1738](https://github.com/apache/incubator-inlong/issues/1738) | [Feature]  InLong audit |

### IMPROVEMENTS:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLLONG-2373](https://github.com/apache/incubator-inlong/issues/2373) | [Improve] Refactor of CreateBusinessWorkflow |
| [INLLONG-2358](https://github.com/apache/incubator-inlong/issues/2358) | [InLong audit] modify audit proxy name of introduction |               
| [INLLONG-2352](https://github.com/apache/incubator-inlong/issues/2352) | [InLong audit] add audit introduction |                                
| [INLLONG-2349](https://github.com/apache/incubator-inlong/issues/2349) | [inlong-dataproxy] change log file name from flum.log to dataproxy.log |
| [INLLONG-2331](https://github.com/apache/incubator-inlong/issues/2331) | [Improve] Extract connector related code to sort-connector module | 
| [INLLONG-2329](https://github.com/apache/incubator-inlong/issues/2329) | [Improve][inlong-dataproxy-sdk] asyncSendMessage in sender.java can be optimized to reduce the number of invalid objects |
| [INLLONG-2297](https://github.com/apache/incubator-inlong/issues/2297) | [Improve][agent] support audit for source and sink |
| [INLLONG-2296](https://github.com/apache/incubator-inlong/issues/2296) | Added lag consumption log |
| [INLLONG-2294](https://github.com/apache/incubator-inlong/issues/2294) | Rename the variable BROKER_VERSION to SERVER_VERSION |
| [INLLONG-2279](https://github.com/apache/incubator-inlong/issues/2279) | [Improve] Supplement TubeMQ's  Javadoc information |
| [INLLONG-2274](https://github.com/apache/incubator-inlong/issues/2274) | [Improve][Manager] Supports configuring whether to create a Hive database or table |
| [INLLONG-2271](https://github.com/apache/incubator-inlong/issues/2271) | [Improve] rename the TDMsg to InLongMsg |
| [INLLONG-2258](https://github.com/apache/incubator-inlong/issues/2258) | [Improve][dashboard] Audit page support auto select datastream |
| [INLLONG-2254](https://github.com/apache/incubator-inlong/issues/2254) | Add historical offset query API |
| [INLLONG-2245](https://github.com/apache/incubator-inlong/issues/2245) | [Improve] Supports database-level isolation of audit queries | 
| [INLLONG-2229](https://github.com/apache/incubator-inlong/issues/2229) | [Improve] Manager support pulsar authentification  | 
| [INLLONG-2225](https://github.com/apache/incubator-inlong/issues/2225) | [Improve][InLong-Dashboard] Audit module support i18n | 
| [INLLONG-2220](https://github.com/apache/incubator-inlong/issues/2220) | [Improve] move dataproxy-sdk to inlong-sdk | 
| [INLLONG-2210](https://github.com/apache/incubator-inlong/issues/2210) | [Improve] package `inlong-manager-web` as `inlong-manager` | 
| [INLLONG-2200](https://github.com/apache/incubator-inlong/issues/2200) | [Feature] DataProxy add supporting to http protocol for reporting data |  
| [INLLONG-2196](https://github.com/apache/incubator-inlong/issues/2196) | [Improve] move website to dashboard |         
| [INLLONG-2193](https://github.com/apache/incubator-inlong/issues/2193) | [Improve] optimize inlong manager structure | 
| [INLLONG-2160](https://github.com/apache/incubator-inlong/issues/2160) | [Improve] Time format conversion using DateTimeFormatter |  
| [INLLONG-2151](https://github.com/apache/incubator-inlong/issues/2151) | [Improve] Add time and sort statistics by topic | 
| [INLLONG-2133](https://github.com/apache/incubator-inlong/issues/2133) | Update year to 2022 | 
| [INLLONG-2126](https://github.com/apache/incubator-inlong/issues/2126) | [Improve]prepare_env.sh can be merged into dataproxy-start.sh，as a InLong beginner maybe forgot this step |
| [INLLONG-2122](https://github.com/apache/incubator-inlong/issues/2122) | [Improve] Send a dev notifications email for issue status |
| [INLLONG-2119](https://github.com/apache/incubator-inlong/issues/2119) | [Improve][Website][CI] Add support for building inlong website when building or testing project |
| [INLLONG-2117](https://github.com/apache/incubator-inlong/issues/2117) | [Improve][agent] optimize class name  |
| [INLLONG-2116](https://github.com/apache/incubator-inlong/issues/2116) | [Improve][Website] Improve the README document |
| [INLLONG-2107](https://github.com/apache/incubator-inlong/issues/2107) | [Improve] [InLong Manager] remove gson and json-simple from dependency |  
| [INLLONG-2103](https://github.com/apache/incubator-inlong/issues/2103) | [Improve] update the definition of Apache InLong | 
| [INLLONG-2073](https://github.com/apache/incubator-inlong/issues/2073) | [Improve] [InLong agent] remove spring 2.5.6 from dependencyManagement |
| [INLLONG-2072](https://github.com/apache/incubator-inlong/issues/2072) | [Improve] update the deployment guide for sort |
| [INLLONG-2070](https://github.com/apache/incubator-inlong/issues/2070) | [Improve] update the default pulsar demo configuration for dataproxy  |
| [INLLONG-1944](https://github.com/apache/incubator-inlong/issues/1944) | Bumped version to 0.13.0-incubating-SNAPSHOT for the master branch |

### BUG FIXES:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLLONG-2371](https://github.com/apache/incubator-inlong/issues/2371) | [Bug][inlong-dataproxy] monitorIndex should not use msgid for key,it affects performance |
| [INLLONG-2361](https://github.com/apache/incubator-inlong/issues/2361) | [Bug] audit have no data |
| [INLLONG-2344](https://github.com/apache/incubator-inlong/issues/2344) | [Bug][InLong-Sort] Kafka sink ut failed under multithread compiling | 
| [INLLONG-2338](https://github.com/apache/incubator-inlong/issues/2338) | [Bug] agent can not get dataproxy for docker-compose environment |
| [INLLONG-2336](https://github.com/apache/incubator-inlong/issues/2336) | [Bug][agent] the manager fetcher thread was shielded |
| [INLLONG-2288](https://github.com/apache/incubator-inlong/issues/2288) | [Bug] sort-flink task catches an NPE |
| [INLLONG-2264](https://github.com/apache/incubator-inlong/issues/2264) | [Bug] DataProxy get metric value with error JMX ObjectName |
| [INLLONG-2263](https://github.com/apache/incubator-inlong/issues/2263) | [Bug] SortStandalone get metric value with error JMX ObjectName |
| [INLLONG-2252](https://github.com/apache/incubator-inlong/issues/2252) | [Bug] Remove <> character in sort-standalone quick_start.md. |     
| [INLLONG-2242](https://github.com/apache/incubator-inlong/issues/2242) | [BUG][manager] table field incorrect: db_collector_detail_task, 'sql' should be 'sql_statement' |
| [INLLONG-2237](https://github.com/apache/incubator-inlong/issues/2237) | [Bug] call audit query interface error |
| [INLLONG-2230](https://github.com/apache/incubator-inlong/issues/2230) | [Bug]  manager started get jackson error |    
| [INLLONG-2227](https://github.com/apache/incubator-inlong/issues/2227) | [Bug] build failed for dataproxy-sdk |
| [INLLONG-2224](https://github.com/apache/incubator-inlong/issues/2224) | [Bug][inlong-DataProxy] Source receive one message will be send to pulsar twice when config both memery channel and file channel | 
| [INLLONG-2202](https://github.com/apache/incubator-inlong/issues/2202) | [Bug] add lower version log4j exclusion in sort-standalone pom.xml |   
| [INLLONG-2199](https://github.com/apache/incubator-inlong/issues/2199) | [Bug][inlong-audit][audit-source] one message will put tow channel, and store two message | 
| [INLLONG-2191](https://github.com/apache/incubator-inlong/issues/2191) | [Bug][inlong-audit][audit-source] requestId is not set in response message |
| [INLLONG-2190](https://github.com/apache/incubator-inlong/issues/2190) | [Bug][inlong-audit][audit-store] can not started by start shell |
| [INLLONG-2174](https://github.com/apache/incubator-inlong/issues/2174) | [Bug]Clickhouse sink can cause data loss when checkpointing | 
| [INLLONG-2155](https://github.com/apache/incubator-inlong/issues/2155) | [Bug][Manager] Some unit tests running failed | 
| [INLLONG-2148](https://github.com/apache/incubator-inlong/issues/2148) | [Bug][sort]Pattern used for extracting clickhouse metadata is not compatible with some versions of clickhouse |
| [INLLONG-2143](https://github.com/apache/incubator-inlong/issues/2143) | [Bug][sort] caught a NoClassDefFoundError exception |
| [INLLONG-2137](https://github.com/apache/incubator-inlong/issues/2137) | [Bug] version 0.12.0 cannot pass UT |
| [INLLONG-2130](https://github.com/apache/incubator-inlong/issues/2130) | [Bug] inlong-sort occurs `ClassNotFoundException: og.objenesis..ClassUtils` | 
| [INLLONG-2113](https://github.com/apache/incubator-inlong/issues/2113) | [Bug][Docker] Audit docker image build failed |
| [INLLONG-2098](https://github.com/apache/incubator-inlong/issues/2098) | [Bug] agent can not restart successfully |
| [INLLONG-2097](https://github.com/apache/incubator-inlong/issues/2097) | [Bug][Docker] error while building tubemq image |
| [INLLONG-2094](https://github.com/apache/incubator-inlong/issues/2094) | [Bug] summit job failed after enabling Prometheus |
| [INLLONG-2089](https://github.com/apache/incubator-inlong/issues/2089) | [Bug]tubemq-manager throws error when starting:   java.lang.ClassNotFoundException: javax.validation.ClockProvider |
| [INLLONG-2087](https://github.com/apache/incubator-inlong/issues/2087) | [Bug] Miss a "-p" flag before 2181:2181 in the command "Start Standalone Container" |
| [INLLONG-2085](https://github.com/apache/incubator-inlong/issues/2085) | [Bug] Solve the incubator-inlong-website Compilation failure problem |
| [INLLONG-2084](https://github.com/apache/incubator-inlong/issues/2084) | [Bug]A bug in the Go SDK demo, and the API result class is not clear enough |
| [INLLONG-2082](https://github.com/apache/incubator-inlong/issues/2082) | [Bug] file agent collector file failed |
| [INLLONG-2080](https://github.com/apache/incubator-inlong/issues/2080) | [Bug] file agent send file failed |
| [INLLONG-2078](https://github.com/apache/incubator-inlong/issues/2078) | [Bug] create pulsar subscription failed |
| [INLLONG-2068](https://github.com/apache/incubator-inlong/issues/2068) | [Bug] the class name in dataproxy stop.sh  is wrong |
| [INLLONG-2066](https://github.com/apache/incubator-inlong/issues/2066) | Each message will be consumed twice.[Bug] |
| [INLLONG-2064](https://github.com/apache/incubator-inlong/issues/2064) | [Bug]master branch, tubemq-manager  module occurs: package Java.validation.constraints not exists |
| [INLLONG-2061](https://github.com/apache/incubator-inlong/issues/2061) | [Bug][Office-Website] The homepage structure image error |
| [INLLONG-1989](https://github.com/apache/incubator-inlong/issues/1989) | [Bug]some font of " DataProxy-SDK architecture " page  incorrectly |
| [INLLONG-1342](https://github.com/apache/incubator-inlong/issues/1342) | [Bug] Create tube consumer group failed where the group exists |  

## Release 0.12.0-incubating - Released (as of 2021-12-22)

### FEATURES:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-1310](https://github.com/apache/incubator-inlong/issues/1310) | [Feature] [Feature] Support Pulsar
| [INLONG-1711](https://github.com/apache/incubator-inlong/issues/1711) | [feature] website support process pulsar dataflow
| [INLONG-1712](https://github.com/apache/incubator-inlong/issues/1712) | [Feature][agent] Add agent metric statistics
| [INLONG-1722](https://github.com/apache/incubator-inlong/issues/1722) | [Feature] Add IssueNavigationLink for IDEA
| [INLONG-1725](https://github.com/apache/incubator-inlong/issues/1725) | [Feature] [InLong-Manager] Modify bid and tid (or dsid) to inlongGroupId and inlongStreamId
| [INLONG-1726](https://github.com/apache/incubator-inlong/issues/1726) | [Feature] [InLong-Website] Adapt the Manager module and modify the field names of bid and dsid
| [INLONG-1732](https://github.com/apache/incubator-inlong/issues/1732) | [Feature] [InLong-Agent] Modify bid and tid to inlongGroupId and inlongStreamId
| [INLONG-1738](https://github.com/apache/incubator-inlong/issues/1738) | [Feature] InLong audit
| [INLONG-1764](https://github.com/apache/incubator-inlong/issues/1764) | [Feature]Use black for code block background style
| [INLONG-1768](https://github.com/apache/incubator-inlong/issues/1768) | [Feature] Adding consume type that allows partition assign from the client
| [INLONG-1785](https://github.com/apache/incubator-inlong/issues/1785) | [Feature] add 0.11.0 release article for blog
| [INLONG-1786](https://github.com/apache/incubator-inlong/issues/1786) | [Feature]Inlong-common provide monitoring indicator reporting mechanism with JMX, user can implement the code that read the metrics and report to user-defined monitor system.
| [INLONG-1791](https://github.com/apache/incubator-inlong/issues/1791) | [Feature][InLong-Manager] Some bid fields have not been modified
| [INLONG-1796](https://github.com/apache/incubator-inlong/issues/1796) | [Feature]DataProxy support monitor indicator with JMX.
| [INLONG-1809](https://github.com/apache/incubator-inlong/issues/1809) | [Feature] Adjust the font style of the official home page
| [INLONG-1814](https://github.com/apache/incubator-inlong/issues/1814) | [Feature] Show document file subdirectories
| [INLONG-1817](https://github.com/apache/incubator-inlong/issues/1817) | [Feature][InLong-Manager] Workflow supports data stream for Pulsar
| [INLONG-1821](https://github.com/apache/incubator-inlong/issues/1821) | [INLONG-810] Sort Module Support store data to ApacheDoris
| [INLONG-1826](https://github.com/apache/incubator-inlong/issues/1826) | [Feature] Use jmx metric defined in inlong-common
| [INLONG-1830](https://github.com/apache/incubator-inlong/issues/1830) | [Feature] Add a star reminder
| [INLONG-1833](https://github.com/apache/incubator-inlong/issues/1833) | [Feature] Add Team button to the navigation bar
| [INLONG-1840](https://github.com/apache/incubator-inlong/issues/1840) | [Feature] add a Welcome committer articles to official website blog
| [INLONG-1847](https://github.com/apache/incubator-inlong/issues/1847) | [Feature][InLong-Manager] Add consumption APIs for Pulsar MQ
| [INLONG-1849](https://github.com/apache/incubator-inlong/issues/1849) | [Feature][InLong-Manager] Push Sort config for Pulsar
| [INLONG-1851](https://github.com/apache/incubator-inlong/issues/1851) | [Feature]TubeMQ supports monitoring indicators with JMX.
| [INLONG-1853](https://github.com/apache/incubator-inlong/issues/1853) | [Feature] Agent should provide docs for jmx metrics
| [INLONG-1854](https://github.com/apache/incubator-inlong/issues/1854) | [Feature] Agent Rmi args should be added in agent-env.sh
| [INLONG-1856](https://github.com/apache/incubator-inlong/issues/1856) | [Feature] Add a news tab on the official website
| [INLONG-1867](https://github.com/apache/incubator-inlong/issues/1867) | [Feature] Add a user column display to the official website
| [INLONG-1873](https://github.com/apache/incubator-inlong/issues/1873) | [Feature] refactor the structure of the document for the official website
| [INLONG-1874](https://github.com/apache/incubator-inlong/issues/1874) | [Feature] Add contact information and common links at the bottom of the homepage of the official website
| [INLONG-1878](https://github.com/apache/incubator-inlong/issues/1878) | [Feature] Optimize user display page layout style
| [INLONG-1901](https://github.com/apache/incubator-inlong/issues/1901) | [Feature] Optimize the layout of the user display page
| [INLONG-1910](https://github.com/apache/incubator-inlong/issues/1910) | [Feature]Inlong-Sort-Standalone-sort-sdk support to consume events from inlong cache clusters(pulsar)
| [INLONG-1926](https://github.com/apache/incubator-inlong/issues/1926) | [Feature]Inlong-Sort-Standalone support JMX metrics listener for pulling.
| [INLONG-1938](https://github.com/apache/incubator-inlong/issues/1938) | [Feature] DataProxy send message to multi-pulsar cluster conf demo
| [INLONG-2002](https://github.com/apache/incubator-inlong/issues/2002) | [Feature]creating data access with pulsar, users should be able to change the ensemble param

### IMPROVEMENTS:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-1708](https://github.com/apache/incubator-inlong/issues/1708) | [Improve] Add restrict of @author and Chinese in java file
| [INLONG-1729](https://github.com/apache/incubator-inlong/issues/1729) | [Improve] Avoid using constant value as version when referencing other modules
| [INLONG-1739](https://github.com/apache/incubator-inlong/issues/1739) | [Improve] Optimization of TubeMQ SDK usage demo
| [INLONG-1740](https://github.com/apache/incubator-inlong/issues/1740) | [Improve] change bid/tid to be more identifiable
| [INLONG-1746](https://github.com/apache/incubator-inlong/issues/1746) | [improve] the log4j properties for dataproxy contains some useless code and some class name are incorrect
| [INLONG-1756](https://github.com/apache/incubator-inlong/issues/1756) | [Improve] Use metadata to manage data sources and flow fields
| [INLONG-1772](https://github.com/apache/incubator-inlong/issues/1772) | [Improve]Adjust the ProcessResult class implementation
| [INLONG-1798](https://github.com/apache/incubator-inlong/issues/1798) | [Improve]RestTemplate does not read configuration from the configuration file
| [INLONG-1802](https://github.com/apache/incubator-inlong/issues/1802) | [Improve] Optimize document version management
| [INLONG-1808](https://github.com/apache/incubator-inlong/issues/1808) | [Improve] Optimize document of DataProxy about monitor metric.
| [INLONG-1810](https://github.com/apache/incubator-inlong/issues/1810) | [Improve] update the architecture for office-website
| [INLONG-1811](https://github.com/apache/incubator-inlong/issues/1811) | [Improve] Modify the architecture diagram of README.md
| [INLONG-1815](https://github.com/apache/incubator-inlong/issues/1815) | [Improve][translation] the blog of the 0.11.0 release should be translated into English
| [INLONG-1819](https://github.com/apache/incubator-inlong/issues/1819) | Optimize GC parameter configuration in TubeMQ's env.sh file
| [INLONG-1822](https://github.com/apache/incubator-inlong/issues/1822) | Optimize the table formatting in some MD documents
| [INLONG-1824](https://github.com/apache/incubator-inlong/issues/1824) | Refine the how-to-vote-a-committer-ppmc.md
| [INLONG-1857](https://github.com/apache/incubator-inlong/issues/1857) | [Improve] Adjust the content of the Disclaimer and Events column
| [INLONG-1859](https://github.com/apache/incubator-inlong/issues/1859) | [Improve][InLong-Manager] Remove duplicate SQL files
| [INLONG-1861](https://github.com/apache/incubator-inlong/issues/1861) | [Improve] Update document for docker-compose
| [INLONG-1863](https://github.com/apache/incubator-inlong/issues/1863) | [Improve][TubeMQ] repHelperHost for master should be exposed in configuration
| [INLONG-1864](https://github.com/apache/incubator-inlong/issues/1864) | [Improve] Agent Website doc contains a typo
| [INLONG-1865](https://github.com/apache/incubator-inlong/issues/1865) | [Improve] There are several errors in TubeMQ's guidance document
| [INLONG-1877](https://github.com/apache/incubator-inlong/issues/1877) | [Improve] improve the document's format for the office website
| [INLONG-1886](https://github.com/apache/incubator-inlong/issues/1886) | [Improve][InLong-Manager] Refactor and delete unused entities
| [INLONG-1916](https://github.com/apache/incubator-inlong/issues/1916) | [Improve][website] modify the Business to InLong Group
| [INLONG-1934](https://github.com/apache/incubator-inlong/issues/1934) | [Improve] update the image of the hive example after the bid changed
| [INLONG-1935](https://github.com/apache/incubator-inlong/issues/1935) | [Improve] package the SQL file for the manager
| [INLONG-1939](https://github.com/apache/incubator-inlong/issues/1939) | [Improve] add basic concepts for InLong
| [INLONG-1952](https://github.com/apache/incubator-inlong/issues/1952) | [Improve] Update the office website structure image
| [INLONG-1987](https://github.com/apache/incubator-inlong/issues/1987) | [Improve] Add function comment information in TubeMQ
| [INLONG-2017](https://github.com/apache/incubator-inlong/issues/2017) | [Improve] Add more guide documents for Pulsar

### BUG FIXES:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-1706](https://github.com/apache/incubator-inlong/issues/1706) | [Bug] there are some incorrect expressions for issues tracking in the how-to-contribute file
| [INLONG-1716](https://github.com/apache/incubator-inlong/issues/1716) | [Bug][manager] can not login successfully
| [INLONG-1731](https://github.com/apache/incubator-inlong/issues/1731) | [Bug] release template has sth wrong with KEY URL
| [INLONG-1745](https://github.com/apache/incubator-inlong/issues/1745) | [Bug]TubeMQ HTTP API download link cannot be opened
| [INLONG-1752](https://github.com/apache/incubator-inlong/issues/1752) | [Bug] The official website action failed to build, it may be that the node version needs to be upgraded
| [INLONG-1754](https://github.com/apache/incubator-inlong/issues/1754) | [Bug] confused navigation in download page
| [INLONG-1755](https://github.com/apache/incubator-inlong/issues/1755) | [Bug] Broken link in the ANNOUNCE email template
| [INLONG-1769](https://github.com/apache/incubator-inlong/issues/1769) | [Bug][TubeMQ]Util function SpitToMap in Go SDK panic
| [INLONG-1771](https://github.com/apache/incubator-inlong/issues/1771) | [Bug] Website readme error
| [INLONG-1776](https://github.com/apache/incubator-inlong/issues/1776) | [Bug] Get error while parse td msg with go client
| [INLONG-1777](https://github.com/apache/incubator-inlong/issues/1777) | [Bug][TubeMQ]Go SDK failed to parse tdmsg v4
| [INLONG-1781](https://github.com/apache/incubator-inlong/issues/1781) | [Bug] Get uncorrect time value of attributes
| [INLONG-1783](https://github.com/apache/incubator-inlong/issues/1783) | [Bug] Topic filters config has't any effects
| [INLONG-1828](https://github.com/apache/incubator-inlong/issues/1828) | [Bug]parse message error: invalid default attr's msg Length
| [INLONG-1876](https://github.com/apache/incubator-inlong/issues/1876) | [Bug] office website build failed
| [INLONG-1897](https://github.com/apache/incubator-inlong/issues/1897) | [Bug][Website] form cannot use chain name
| [INLONG-1898](https://github.com/apache/incubator-inlong/issues/1898) | [Bug][Website] The error of the person responsible for the second edit of the new consumption
| [INLONG-1902](https://github.com/apache/incubator-inlong/issues/1902) | [Bug][Website] Access create params error
| [INLONG-1911](https://github.com/apache/incubator-inlong/issues/1911) | [Bug] Some questions about the metric implementation in the common module
| [INLONG-1915](https://github.com/apache/incubator-inlong/issues/1915) | [Bug] tubemq master can not start
| [INLONG-1919](https://github.com/apache/incubator-inlong/issues/1919) | [Bug] TubeMQ HTTP API xls can not download
| [INLONG-1920](https://github.com/apache/incubator-inlong/issues/1920) | [Bug]Failed to start up MultiSession factory by following the demo code
| [INLONG-1953](https://github.com/apache/incubator-inlong/issues/1953) | [Bug]It can not be submitted when I create data access using file data source
| [INLONG-1954](https://github.com/apache/incubator-inlong/issues/1954) | [Bug]inlong-sort does not support pulsar ???
| [INLONG-1955](https://github.com/apache/incubator-inlong/issues/1955) | [Bug]Source data fields' type are all mapped to tinyint, and can not be modified
| [INLONG-1958](https://github.com/apache/incubator-inlong/issues/1958) | [Bug]Avoid the security risks of log4j package
| [INLONG-1966](https://github.com/apache/incubator-inlong/issues/1966) | [Bug][InLong-Manager] The stream name field is not required, but error occurs when create a data stream with name field not filled
| [INLONG-1967](https://github.com/apache/incubator-inlong/issues/1967) | [Bug][InLong-Manager] Cannot create the Pulsar subscription
| [INLONG-1973](https://github.com/apache/incubator-inlong/issues/1973) | [Bug]with the demo conf of dataproxy, the app can not start rightly
| [INLONG-1975](https://github.com/apache/incubator-inlong/issues/1975) | [Bug]error occurs when deleting a data access
| [INLONG-1978](https://github.com/apache/incubator-inlong/issues/1978) | [Bug]Create multiple file import tasks, and inlong-agent reports an error when registering metric
| [INLONG-1980](https://github.com/apache/incubator-inlong/issues/1980) | [Bug]the content of topics.properties generated incorrectly，and too much backup files generate automatically
| [INLONG-1981](https://github.com/apache/incubator-inlong/issues/1981) | [Bug] When compiling the project, the InLong-audit module reported Warning errors
| [INLONG-1984](https://github.com/apache/incubator-inlong/issues/1984) | [Bug][InLong-Manager] Create pulsar access, modify pulsar related parameters failed
| [INLONG-1995](https://github.com/apache/incubator-inlong/issues/1995) | [Bug] Compile Audit-SDK and report TestNGException
| [INLONG-1996](https://github.com/apache/incubator-inlong/issues/1996) | [Bug] Compile the project and InLong-Agent module throws 3 exceptions
| [INLONG-1997](https://github.com/apache/incubator-inlong/issues/1997) | [Bug]after the compilation of inlong, no lib directory in inlong-dataproxy
| [INLONG-2009](https://github.com/apache/incubator-inlong/issues/2009) | [Bug]Topic obtained through "/api/inlong/manager/openapi/dataproxy/getConfig" is not right
| [INLONG-2012](https://github.com/apache/incubator-inlong/issues/2012) | [Bug] Inlong-agent could not fetch file agent task through api --"/api/inlong/manager/openapi/agent/fileAgent/getTaskConf"
| [INLONG-2014](https://github.com/apache/incubator-inlong/issues/2014) | [Bug]inlong-dataproxy could not identify the groupId and topic format of topics.properties
| [INLONG-2018](https://github.com/apache/incubator-inlong/issues/2018) | [Bug]after approving a data access, some failures happen and the data access is always in the state of configuration
| [INLONG-2020](https://github.com/apache/incubator-inlong/issues/2020) | [Bug] Dependency of "jul-to-slf4j" is missing for pulsar connector
| [INLONG-2023](https://github.com/apache/incubator-inlong/issues/2023) | [Bug] Agent stream id is not passed to proxy
| [INLONG-2026](https://github.com/apache/incubator-inlong/issues/2026) | [Bug] Found Pulsar client create failure when starting Sort
| [INLONG-2030](https://github.com/apache/incubator-inlong/issues/2030) | [Bug]inlong-agent raises NPE error when running
| [INLONG-2032](https://github.com/apache/incubator-inlong/issues/2032) | [Bug]"javax.xml.parsers.FactoryConfigurationError" throwed in flink when starting a inlong-sort job
| [INLONG-2035](https://github.com/apache/incubator-inlong/issues/2035) | [Bug] Agent use wrong tid __ to generate message
| [INLONG-2038](https://github.com/apache/incubator-inlong/issues/2038) | [Bug]inlong-sort abandon data from pulsar due to an ClassCastException
| [INLONG-2043](https://github.com/apache/incubator-inlong/issues/2043) | [Bug] Sort module renames tid to streamId


## Release 0.11.0-incubating - Released (as of 2021-10-25)

### FEATURES:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-624](https://github.com/apache/incubator-inlong/issues/624) | [Feature] Go SDK support for TubeMQ
| [INLONG-1308](https://github.com/apache/incubator-inlong/issues/1308) | [Feature] Support Deploying All Modules on Kubernetes
| [INLONG-1330](https://github.com/apache/incubator-inlong/issues/1330) | [Feature] DataProxy support Pulsar
| [INLONG-1631](https://github.com/apache/incubator-inlong/issues/1631) | [Feature] [office-website] Refactor incubator-inlong-website by docusaurus

### IMPROVEMENTS:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-1324](https://github.com/apache/incubator-inlong/issues/1324) | [Improve] [Manager] The consumption details should be refreshed after editing successfully
| [INLONG-1327](https://github.com/apache/incubator-inlong/issues/1327) | [Improve] [Manager] Supports pagi`ng query for workflow execution log
| [INLONG-1578](https://github.com/apache/incubator-inlong/issues/1570) | [Improve] Go SDK should provide a more elegant way to set the parameter of config`
| [INLONG-1571](https://github.com/apache/incubator-inlong/issues/1571) | [Improve] [CI] Check License Heade
| [INLONG-1584](https://github.com/apache/incubator-inlong/issues/1584) | [Improve] TDMsg Decode Support For Go SDK
| [INLONG-1585](https://github.com/apache/incubator-inlong/issues/1585) | [Improve] Improve issue template with issue forms
| [INLONG-1589](https://github.com/apache/incubator-inlong/issues/1589) | [Improve] [Manager] Manager provide an openapi of DataProxy configuration data for multi-subcluster
| [INLONG-1593](https://github.com/apache/incubator-inlong/issues/1593) | [Improve] Add EmptyLineSeparator java code checkstyle rule
| [INLONG-1595](https://github.com/apache/incubator-inlong/issues/1595) | [Improve] inlong-dataproxy start by the configuration data from inlong-manager
| [INLONG-1623](https://github.com/apache/incubator-inlong/issues/1602) | [Improve] Optimize EntityStatus enum
| [INLONG-1619](https://github.com/apache/incubator-inlong/issues/1619) | [Improve] Add improve suggestion template
| [INLONG-1611](https://github.com/apache/incubator-inlong/issues/1611) | [Improve] Enable Merge Button
| [INLONG-1623](https://github.com/apache/incubator-inlong/issues/1623) | [Improve] add contribution guide document for the main repo
| [INLONG-1626](https://github.com/apache/incubator-inlong/issues/1626) | [Improve] [office-website] Agent introduce a Message filter
| [INLONG-1628](https://github.com/apache/incubator-inlong/issues/1628) | [Improve] Remove commitlint in package.json
| [INLONG-1629](https://github.com/apache/incubator-inlong/issues/1629) | [Improve] Disable merge commit
| [INLONG-1632](https://github.com/apache/incubator-inlong/issues/1632) | [Improve] [office-website] Refactoring of basic projects
| [INLONG-1633](https://github.com/apache/incubator-inlong/issues/1633) | [Improve] [office-website] Migrate modules documentation
| [INLONG-1634](https://github.com/apache/incubator-inlong/issues/1634) | [Improve] [office-website] Migrate download documentation
| [INLONG-1635](https://github.com/apache/incubator-inlong/issues/1635) | [Improve] [office-website] Migrate development documentation
| [INLONG-1636](https://github.com/apache/incubator-inlong/issues/1636) | [Improve] [office-website] Replace the default language selection icon
| [INLONG-1637](https://github.com/apache/incubator-inlong/issues/1637) | [Improve] [office-website] Add docusaurus i18n docs
| [INLONG-1638](https://github.com/apache/incubator-inlong/issues/1638) | [Improve] [office-website] Adapt new github action command
| [INLONG-1641](https://github.com/apache/incubator-inlong/issues/1641) | [Improve] [Agent] Agent introduce a Message filter
| [INLONG-1642](https://github.com/apache/incubator-inlong/issues/1642) | [Improve] [Agent] Agent Use Message filter to get tid from different lines in a file
| [INLONG-1650](https://github.com/apache/incubator-inlong/issues/1650) | [Improve] [TubeMQ] Provide a more elegant way to define config address
| [INLONG-1662](https://github.com/apache/incubator-inlong/issues/1662) | [Improve] [GitHub] Improve issue templates
| [INLONG-1666](https://github.com/apache/incubator-inlong/issues/1666) | [Improve] [TubeMQ] README for Go SDK
| [INLONG-1668](https://github.com/apache/incubator-inlong/issues/1668) | [Improve] [office-website] Adapt quick edit link
| [INLONG-1669](https://github.com/apache/incubator-inlong/issues/1669) | [Improve] [office-website] Adapt docusaurus build command
| [INLONG-1670](https://github.com/apache/incubator-inlong/issues/1670) | [Improve] [Manager] Add H2 in UT
| [INLONG-1680](https://github.com/apache/incubator-inlong/issues/1680) | [Improve] [doc] remove the redundant download links
| [INLONG-1682](https://github.com/apache/incubator-inlong/issues/1682) | [Improve] [TubeMQ] New Go module for Go SDK
| [INLONG-1699](https://github.com/apache/incubator-inlong/issues/1699) | [Improve] [doc] add a correct interpretation for InLong
| [INLONG-1701](https://github.com/apache/incubator-inlong/issues/1701) | [Improve] [InLong-Manager] Adjust unit tests

### BUG FIXES:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-1498](https://github.com/apache/incubator-inlong/issues/1498) | ignore the files with versionsBackup suffix for the bumped version
| [INLONG-1507](https://github.com/apache/incubator-inlong/issues/1507) | Go Client should reconnect to server if the server is shutdown and restarted
| [INLONG-1509](https://github.com/apache/incubator-inlong/issues/1509) | duplicate issues be counted in CHANGES.md
| [INLONG-1511](https://github.com/apache/incubator-inlong/issues/1511) | release guild documents has some errors
| [INLONG-1514](https://github.com/apache/incubator-inlong/issues/1514) | the license header is not correct for inlong-website/nginx.conf
| [INLONG-1525](https://github.com/apache/incubator-inlong/issues/1525) | Go SDK fail to parse SubscribeInfo
| [INLONG-1527](https://github.com/apache/incubator-inlong/issues/1527) | GoSDK should throw error if it fail to connect to master
| [INLONG-1529](https://github.com/apache/incubator-inlong/issues/1529) | Go SDK should reset heartbeat if register to master successfully
| [INLONG-1531](https://github.com/apache/incubator-inlong/issues/1531) | Go SDK should init the flow control item of the partition
| [INLONG-1533](https://github.com/apache/incubator-inlong/issues/1533) | Go SDK should provide more example
| [INLONG-1535](https://github.com/apache/incubator-inlong/issues/1535) | Go SDK should be closed before stopping the event processing goroutine
| [INLONG-1538](https://github.com/apache/incubator-inlong/issues/1538) | TubeMQ reports the error "Topic xxx not publish" when producing data
| [INLONG-1550](https://github.com/apache/incubator-inlong/issues/1550) | Go SDK should obey the flow control rule
| [INLONG-1552](https://github.com/apache/incubator-inlong/issues/1552) | Java SDK should deal with the default flow control rule
| [INLONG-1553](https://github.com/apache/incubator-inlong/issues/1553) | migrate the user manual documents at first class
| [INLONG-1554](https://github.com/apache/incubator-inlong/issues/1554) | remove the Console Introduction for manager
| [INLONG-1555](https://github.com/apache/incubator-inlong/issues/1555) | Go SDK should record the consumer config to the log
| [INLONG-1558](https://github.com/apache/incubator-inlong/issues/1558) | Go SDK should provide a multi goroutine consumer example
| [INLONG-1560](https://github.com/apache/incubator-inlong/issues/1560) | C++ SDK can not return error code of PartInUse and PartWaiting correctly
| [INLONG-1562](https://github.com/apache/incubator-inlong/issues/1562) | [K8s] There are some syntax bugs and configuration bugs in helm chart
| [INLONG-1563](https://github.com/apache/incubator-inlong/issues/1563) | Go SDK can not stop the heartbeat timer after the consumer has been closed
| [INLONG-1566](https://github.com/apache/incubator-inlong/issues/1566) | The user defined partition offset of Go SDK can not take effect
| [INLONG-1568](https://github.com/apache/incubator-inlong/issues/1568) | C++ SDK cant not return the whether the partition has been registered correctly
| [INLONG-1569](https://github.com/apache/incubator-inlong/issues/1569) | The first_registered is not the same with its naming
| [INLONG-1573](https://github.com/apache/incubator-inlong/issues/1573) | Add TDMsg decode logic to TubeMQ's C++ SDK
| [INLONG-1575](https://github.com/apache/incubator-inlong/issues/1575) | Modify the download url of version 0.9.0
| [INLONG-1579](https://github.com/apache/incubator-inlong/issues/1579) | lots of files are not standard License Header
| [INLONG-1581](https://github.com/apache/incubator-inlong/issues/1581) | InLong's website does not work without Javascript
| [INLONG-1587](https://github.com/apache/incubator-inlong/issues/1587) | Fix compile error
| [INLONG-1592](https://github.com/apache/incubator-inlong/issues/1592) | TextFileReader: The cpu utilization rate is very high, nearly 50%
| [INLONG-1600](https://github.com/apache/incubator-inlong/issues/1600) | There are some YAML errors in bug report and feature request issue forms
| [INLONG-1604](https://github.com/apache/incubator-inlong/issues/1604) | Some resultType is wrong in mapper
| [INLONG-1607](https://github.com/apache/incubator-inlong/issues/1607) | The master version should be added in the bug-report.yml
| [INLONG-1614](https://github.com/apache/incubator-inlong/issues/1614) | dataProxyConfigRepository constructor error
| [INLONG-1617](https://github.com/apache/incubator-inlong/issues/1617) | Ignore mysql directory after run docker compose
| [INLONG-1621](https://github.com/apache/incubator-inlong/issues/1621) | RestTemplateConfig cannot load config from properties
| [INLONG-1625](https://github.com/apache/incubator-inlong/issues/1625) | some page links are not available for Contribution Guide
| [INLONG-1645](https://github.com/apache/incubator-inlong/issues/1645) | [Bug] Druid datasource is not used
| [INLONG-1665](https://github.com/apache/incubator-inlong/issues/1665) | Adjust the content of the document title
| [INLONG-1673](https://github.com/apache/incubator-inlong/issues/1673) | some links are not available after office-website refactored
| [INLONG-1676](https://github.com/apache/incubator-inlong/issues/1676) | two recent PRs were overwritten after the office-website refactored
| [INLONG-1677](https://github.com/apache/incubator-inlong/issues/1677) | the architecture picture is lost in README
| [INLONG-1685](https://github.com/apache/incubator-inlong/issues/1685) | the Chinese Quick Start Guide has some incorrect place after the office-webiste refactored
| [INLONG-1694](https://github.com/apache/incubator-inlong/issues/1694) | Build docker mirror error for TubeMQ C++
| [INLONG-1695](https://github.com/apache/incubator-inlong/issues/1695) | [Bug][DataProxy] Build failed

## Release 0.10.0-incubating - Released (as of 2021-09-01)

### IMPROVEMENTS:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-570](https://issues.apache.org/jira/browse/INLONG-570) | Optimizing the implementations of HTTP API for Master
| [INLONG-726](https://issues.apache.org/jira/browse/INLONG-726) | Optimize The Deployment For InLong
| [INLONG-732](https://issues.apache.org/jira/browse/INLONG-732) | Optimize the CI/CD workflow for build and integration test
| [INLONG-733](https://issues.apache.org/jira/browse/INLONG-733) | Unity the Manger-API/Manger-OpenAPI, and change Manager-API to Manager-Web
| [INLONG-744](https://issues.apache.org/jira/browse/INLONG-744) | Error log, should use log4j
| [INLONG-750](https://issues.apache.org/jira/browse/INLONG-750) | Add configuration descriptions for 'defEthName' in 'broker.ini'
| [INLONG-756](https://issues.apache.org/jira/browse/INLONG-756) | package start.sh script executable for agent
| [INLONG-768](https://issues.apache.org/jira/browse/INLONG-768) | add github pull request template
| [INLONG-789](https://issues.apache.org/jira/browse/INLONG-789) | make agent readme more friendly
| [INLONG-792](https://issues.apache.org/jira/browse/INLONG-792) | tubemanager add a cluster after configuration
| [INLONG-800](https://issues.apache.org/jira/browse/INLONG-800) | Fix codestyle of some comments and methods names.
| [INLONG-804](https://issues.apache.org/jira/browse/INLONG-804) | Optimize the ASF Configuration
| [INLONG-805](https://issues.apache.org/jira/browse/INLONG-805) | Migrate InLong Issues from JIRA to GitHub
| [INLONG-808](https://issues.apache.org/jira/browse/INLONG-808) | Missing dataproxy sdk readme
| [INLONG-809](https://issues.apache.org/jira/browse/INLONG-809) | dataproxy readme delete reference url
| [INLONG-1498](https://github.com/apache/incubator-inlong/issues/1498) |  ignore the files with versionsBackup suffix for the bumped version
| [INLONG-1487](https://github.com/apache/incubator-inlong/issues/1487) |  remove the user number limit  when create a new data stream
| [INLONG-1486](https://github.com/apache/incubator-inlong/issues/1486) |  [agent] update the document about configuring the dataprxy address
| [INLONG-1485](https://github.com/apache/incubator-inlong/issues/1485) |  [sort] add the guide documents for using Pulsar
| [INLONG-1484](https://github.com/apache/incubator-inlong/issues/1484) |  Bid and Tid is not well explained in agent and might cause send error
| [INLONG-1464](https://github.com/apache/incubator-inlong/issues/1464) |  Add code CheckStyle rules
| [INLONG-1459](https://github.com/apache/incubator-inlong/issues/1459) |  proxy address configuration is redundant for inlong-agent
| [INLONG-1457](https://github.com/apache/incubator-inlong/issues/1457) |  remove the user limit for creating a new data access
| [INLONG-1455](https://github.com/apache/incubator-inlong/issues/1455) |  add a script to publish docker images
| [INLONG-1443](https://github.com/apache/incubator-inlong/issues/1443) |   Provide management interface SDK
| [INLONG-1439](https://github.com/apache/incubator-inlong/issues/1439) |  Add the port legal check and remove the useless deleteWhen field
| [INLONG-1430](https://github.com/apache/incubator-inlong/issues/1430) |  Go SDK example
| [INLONG-1429](https://github.com/apache/incubator-inlong/issues/1429) |  update the asf config for inlong office website
| [INLONG-1427](https://github.com/apache/incubator-inlong/issues/1427) |  Go SDK return maxOffset and updateTime in ConsumerOffset
| [INLONG-1424](https://github.com/apache/incubator-inlong/issues/1424) |  change the format of the configuration file: make the yaml to properties
| [INLONG-1423](https://github.com/apache/incubator-inlong/issues/1423) |  modify the docker image of the inlong-manager module
| [INLONG-1417](https://github.com/apache/incubator-inlong/issues/1417) |  rename the distribution file for inlong
| [INLONG-1415](https://github.com/apache/incubator-inlong/issues/1415) |  [TubeMQ Docker] expose zookeeper port for other component usages
| [INLONG-1409](https://github.com/apache/incubator-inlong/issues/1409) |  Sort out the LICENSE information of the 3rd-party components that the DataProxy submodule depends on
| [INLONG-1407](https://github.com/apache/incubator-inlong/issues/1407) |  [DataProxy]Adjust the pom dependency of the DataProxy module
| [INLONG-1405](https://github.com/apache/incubator-inlong/issues/1405) |  too many issues mail at dev@inlong mailbox

### BUG FIXES:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-751](https://issues.apache.org/jira/browse/INLONG-751) | InLong Manager start up error
| [INLONG-776](https://issues.apache.org/jira/browse/INLONG-776) | fix the version error for tubemq cpp client docker image
| [INLONG-777](https://issues.apache.org/jira/browse/INLONG-777) | InLong Manager new data stream error
| [INLONG-782](https://issues.apache.org/jira/browse/INLONG-782) | Optimize The PULL_REQUEST_TEMPLATE
| [INLONG-787](https://issues.apache.org/jira/browse/INLONG-787) | The actions "reviewdog/action-setup" is not allowed to be used
| [INLONG-797](https://issues.apache.org/jira/browse/INLONG-797) | the document for deployment DataProxy is not complete
| [INLONG-799](https://issues.apache.org/jira/browse/INLONG-799) | can not find common.properties for dataproxy
| [INLONG-1488](https://github.com/apache/incubator-inlong/issues/1488) |  there are still some chinese characters for website
| [INLONG-1475](https://github.com/apache/incubator-inlong/issues/1475) |  Tube manager compile ch.qos.logback with error
| [INLONG-1474](https://github.com/apache/incubator-inlong/issues/1474) |  the interface of get data proxy configurations got abnormal status result
| [INLONG-1470](https://github.com/apache/incubator-inlong/issues/1470) |  Java.util.ConcurrentModificationException error when rebalance
| [INLONG-1468](https://github.com/apache/incubator-inlong/issues/1468) |  The update interval of dataproxy is quite long and may cause produce error when config is not updated
| [INLONG-1466](https://github.com/apache/incubator-inlong/issues/1466) |  get snappy error when the agent collecting data
| [INLONG-1462](https://github.com/apache/incubator-inlong/issues/1462) |  dataproxy can not create configuration properties successfully in the docker container
| [INLONG-1458](https://github.com/apache/incubator-inlong/issues/1458) |  The http port in agent readme should be 8008 to be consistent with the code
| [INLONG-1453](https://github.com/apache/incubator-inlong/issues/1453) |  agent connect dataproxy fail when using docker-compose
| [INLONG-1448](https://github.com/apache/incubator-inlong/issues/1448) |  The Manager throws an exception when creating a business
| [INLONG-1447](https://github.com/apache/incubator-inlong/issues/1447) |  Fix Group Control API logic bug
| [INLONG-1444](https://github.com/apache/incubator-inlong/issues/1444) |  Fix Web API multiple field search logic bug
| [INLONG-1441](https://github.com/apache/incubator-inlong/issues/1441) |  Repair Broker configuration API bugs
| [INLONG-1436](https://github.com/apache/incubator-inlong/issues/1436) |  [CI] The checkstyle workflow is redundant
| [INLONG-1432](https://github.com/apache/incubator-inlong/issues/1432) |  The manager url of agent and dataproxy need to be updated since manager merged openapi and api into one module
| [INLONG-1403](https://github.com/apache/incubator-inlong/issues/1403) |  fix some error in dataproxy-sdk readme

### SUB-TASK:
| ISSUE  | Summary  |
| :---- | :------- |
| [INLONG-576](https://issues.apache.org/jira/browse/INLONG-576) | Build metadata entity classes
| [INLONG-578](https://issues.apache.org/jira/browse/INLONG-578) | Build implementation classes based on BDB storage
| [INLONG-579](https://issues.apache.org/jira/browse/INLONG-579) | Add structure mapping of BDB and metadata entity classes
| [INLONG-580](https://issues.apache.org/jira/browse/INLONG-580) | Build active and standby keep-alive services
| [INLONG-581](https://issues.apache.org/jira/browse/INLONG-581) | Add data cache in BDB metadata Mapper implementations
| [INLONG-582](https://issues.apache.org/jira/browse/INLONG-582) | Adjust the business logic related to the BdbClusterSettingEntity class
| [INLONG-583](https://issues.apache.org/jira/browse/INLONG-583) | Adjust BrokerConfManager class implementation
| [INLONG-584](https://issues.apache.org/jira/browse/INLONG-584) | Adjust WebMasterInfoHandler class implementation
| [INLONG-593](https://issues.apache.org/jira/browse/INLONG-593) | Add WebGroupConsumeCtrlHandler class implementation
| [INLONG-595](https://issues.apache.org/jira/browse/INLONG-595) | Add WebBrokerConfHandler class implementation
| [INLONG-596](https://issues.apache.org/jira/browse/INLONG-596) | Add WebTopicConfHandler class implementation
| [INLONG-597](https://issues.apache.org/jira/browse/INLONG-597) | Adjust WebTopicCtrlHandler class implementation
| [INLONG-598](https://issues.apache.org/jira/browse/INLONG-598) | Adjust WebTopicCtrlHandler class implementation
| [INLONG-599](https://issues.apache.org/jira/browse/INLONG-599) | Adjust WebParameterUtils.java's static functions
| [INLONG-601](https://issues.apache.org/jira/browse/INLONG-601) | Adjust WebBrokerDefConfHandler class implementation
| [INLONG-602](https://issues.apache.org/jira/browse/INLONG-602) | Add replacement processing after metadata changes
| [INLONG-611](https://issues.apache.org/jira/browse/INLONG-611) | Add FSM for broker configure manage
| [INLONG-617](https://issues.apache.org/jira/browse/INLONG-617) | Add unit tests for WebParameterUtils
| [INLONG-618](https://issues.apache.org/jira/browse/INLONG-618) | Add unit tests for metastore.dao.entity.*
| [INLONG-625](https://issues.apache.org/jira/browse/INLONG-625) | Add unit tests for metamanage.metastore.impl.*
| [INLONG-626](https://issues.apache.org/jira/browse/INLONG-626) | Fix broker and topic confiugre implement bugs
| [INLONG-707](https://issues.apache.org/jira/browse/INLONG-707) | Bumped version to 0.10.0-SNAPSHOT
| [INLONG-740](https://issues.apache.org/jira/browse/INLONG-740) | Merge the changes in INLONG-739 to master and delete the temporary branch
| [INLONG-755](https://issues.apache.org/jira/browse/INLONG-755) | Go SDK Consumer Result
| [INLONG-757](https://issues.apache.org/jira/browse/INLONG-757) | fix the artifactId of dataproxy
| [INLONG-758](https://issues.apache.org/jira/browse/INLONG-758) | remove redundant baseDirectory for manager output files
| [INLONG-759](https://issues.apache.org/jira/browse/INLONG-759) | fix assembly issue for TubeMQ manager
| [INLONG-760](https://issues.apache.org/jira/browse/INLONG-760) | standardize the directories name for the sort sub-module
| [INLONG-761](https://issues.apache.org/jira/browse/INLONG-761) | unify all modules target files to a singe directory
| [INLONG-762](https://issues.apache.org/jira/browse/INLONG-762) | refactor the deployment document
| [INLONG-763](https://issues.apache.org/jira/browse/INLONG-763) | make the inlong-websit be a maven module of InLong project
| [INLONG-764](https://issues.apache.org/jira/browse/INLONG-764) | Fix Go SDK RPC Request bug
| [INLONG-766](https://issues.apache.org/jira/browse/INLONG-766) | Fix Go SDK Codec Bug
| [INLONG-770](https://issues.apache.org/jira/browse/INLONG-770) | update the readme document
| [INLONG-771](https://issues.apache.org/jira/browse/INLONG-771) | Fix Go SDK Authorization Bug
| [INLONG-773](https://issues.apache.org/jira/browse/INLONG-773) | add manager docker image
| [INLONG-774](https://issues.apache.org/jira/browse/INLONG-774) | update TubeMQ docker images to InLong repo
| [INLONG-778](https://issues.apache.org/jira/browse/INLONG-778) | Fix Go SDK Consumer Bug
| [INLONG-779](https://issues.apache.org/jira/browse/INLONG-779) | update tubemq manager docker image
| [INLONG-781](https://issues.apache.org/jira/browse/INLONG-781) | add inlong agent docker image support
| [INLONG-784](https://issues.apache.org/jira/browse/INLONG-784) | Fix Go SDK Heartbeat Bug
| [INLONG-785](https://issues.apache.org/jira/browse/INLONG-785) | Fix Go SDK Metadata Bug
| [INLONG-786](https://issues.apache.org/jira/browse/INLONG-786) | add dataproxy docker image
| [INLONG-788](https://issues.apache.org/jira/browse/INLONG-788) | Fix Go SDK Remote Cache Bug
| [INLONG-791](https://issues.apache.org/jira/browse/INLONG-791) | Go SDK Support multiple topic address
| [INLONG-793](https://issues.apache.org/jira/browse/INLONG-793) | Fix Some Corner Case in Go SDK
| [INLONG-794](https://issues.apache.org/jira/browse/INLONG-794) | add website docker image
| [INLONG-803](https://issues.apache.org/jira/browse/INLONG-803) | add docker requirement for building InLong
| [INLONG-806](https://issues.apache.org/jira/browse/INLONG-806) | open GitHub Issue For InLong
| [INLONG-807](https://issues.apache.org/jira/browse/INLONG-807) | migrate issue history to inlong
| [INLONG-1491](https://github.com/apache/incubator-inlong/issues/1491) |  Add 0.10.0 version release modification to CHANGES.md
| [INLONG-1492](https://github.com/apache/incubator-inlong/issues/1492) |  Bumped version to 0.11.0-incubating-SNAPSHOT
| [INLONG-1493](https://github.com/apache/incubator-inlong/issues/1493) |  Modify download&release notes for 0.10.0
| [INLONG-1494](https://github.com/apache/incubator-inlong/issues/1494) |  Adjust the version information of all pom.xml to 0.10.0-incubating
| [INLONG-1495](https://github.com/apache/incubator-inlong/issues/1495) |  Update Office website content for release 0.10.0
| [INLONG-1496](https://github.com/apache/incubator-inlong/issues/1496) |  Release InLong 0.10.0
| [INLONG-1497](https://github.com/apache/incubator-inlong/issues/1497) |  create a 0.10.0 branch to release
| [INLONG-1502](https://github.com/apache/incubator-inlong/issues/1502) |  publish all 0.10.0 images to docker hub

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
| [INLONG-743](https://issues.apache.org/jira/browse/INLONG-743) | Adjust the rat check setting of the pom.xml  | Major |

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
