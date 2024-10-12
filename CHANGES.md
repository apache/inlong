
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

# Release InLong 2.0.0 - Released (as of 2024-10-12)
### Agent
|ISSUE|Summary|
|:--|:--|
|[INLONG-10286](https://github.com/apache/inlong/issues/10286)|[Improve][Agent] Update the MQTT Source|
|[INLONG-10287](https://github.com/apache/inlong/issues/10287)|[Improve][Agent] Update the Redis Source|
|[INLONG-10288](https://github.com/apache/inlong/issues/10288)|[Improve][Agent] Update the Oracle Source|
|[INLONG-10289](https://github.com/apache/inlong/issues/10289)|[Improve][Agent] Update the SQLServer Source |
|[INLONG-10742](https://github.com/apache/inlong/issues/10742)|[Improve][Agent] Set the formatting of unix timestamp for SQLServerSource as optional|
|[INLONG-10749](https://github.com/apache/inlong/issues/10749)|[Improve][Agent] Local DB instance records may leak|
|[INLONG-10751](https://github.com/apache/inlong/issues/10751)|[Improve][Agent] Agent needs to report process status for backend problem analysis|
|[INLONG-10756](https://github.com/apache/inlong/issues/10756)|[Improve][Agent] Report file metrics for backend problem analysis|
|[INLONG-10761](https://github.com/apache/inlong/issues/10761)|[Improve][Agent] Delete reader related code |
|[INLONG-10770](https://github.com/apache/inlong/issues/10770)|[Improve][Agent] Delete audit proxy configuration related code|
|[INLONG-10889](https://github.com/apache/inlong/issues/10889)|[Improve][Agent] When detecting oom, it is recommended to exit the process|
|[INLONG-11135](https://github.com/apache/inlong/issues/11135)|[Improve][Agent] Support filtering capability when supplementing data|
|[INLONG-11145](https://github.com/apache/inlong/issues/11145)|[Improve][Agent] Data supplementation needs optimization|
|[INLONG-11179](https://github.com/apache/inlong/issues/11179)|[Improve][Agent] Delete useless code|
|[INLONG-11298](https://github.com/apache/inlong/issues/11298)|[Improve][Agent] Pulsar source needs to do some exception handling |
|[INLONG-11306](https://github.com/apache/inlong/issues/11306)|[Improve][Agent] Modify the naming of variables in the redis source|
|[INLONG-11327](https://github.com/apache/inlong/issues/11327)|[Improve][Agent] No longer retrieve extend class from task configuration|
|[INLONG-11333](https://github.com/apache/inlong/issues/11333)|[Improve][Agent] Retrieve IP from configuration file during audit reporting|
|[INLONG-11175](https://github.com/apache/inlong/issues/11175)|[Bug][Agent] The current MqttSource solution may cause message loss|

### Dashboard
|ISSUE|Summary|
|:--|:--|
|[INLONG-10632](https://github.com/apache/inlong/issues/10632)|[Feature][Dashboard] dashboard should support oceanbase|
|[INLONG-10693](https://github.com/apache/inlong/issues/10693)|[Improve][Dashboard] Added custom ASCII code option for source data field separator|
|[INLONG-10779](https://github.com/apache/inlong/issues/10779)|[Improve][Dashboard] Data synchronization adds offline synchronization configuration|
|[INLONG-10783](https://github.com/apache/inlong/issues/10783)|[Improve][Dashboard] Add enableCreateResource field to all sinks|
|[INLONG-10787](https://github.com/apache/inlong/issues/10787)|[Improve][Dashboard] Update missing i18n item|
|[INLONG-10839](https://github.com/apache/inlong/issues/10839)|[Improve][Dashboard] Data preview style structure modification|
|[INLONG-10844](https://github.com/apache/inlong/issues/10844)|[Improve][Dashboard] Resource details page sort information interface switch|
|[INLONG-10846](https://github.com/apache/inlong/issues/10846)|[Improve][Dashboard] Add cluster name to data source information display|
|[INLONG-10847](https://github.com/apache/inlong/issues/10847)|[Improve][Dashboard] Cluster node management adds heartbeat display page|
|[INLONG-10910](https://github.com/apache/inlong/issues/10910)|[Improve][Dashboard] Sort info Modify some field names|
|[INLONG-11178](https://github.com/apache/inlong/issues/11178)|[Improve][Dashboard]  Added http sink and http node|
|[INLONG-11183](https://github.com/apache/inlong/issues/11183)|[Improve][Dashboard] Module audit page indicator items are merged with other items|
|[INLONG-11197](https://github.com/apache/inlong/issues/11197)|[Improve][Dashboard] Add delete button to cluster management and template management|
|[INLONG-11204](https://github.com/apache/inlong/issues/11204)|[Improve][Dashboard] agent node hidden port number|
|[INLONG-11210](https://github.com/apache/inlong/issues/11210)|[Improve][Dashboard] Version management removes related interface calls and adds default values|
|[INLONG-10572](https://github.com/apache/inlong/issues/10572)|[Bug][Dashboard] The Approvals page appears "Something went wrong"|
|[INLONG-10947](https://github.com/apache/inlong/issues/10947)|[Bug][Dashboard] Modify field description missing for English locale|
|[INLONG-10973](https://github.com/apache/inlong/issues/10973)|[Bug][Dashboard] A wrong error occurs in data preview|

### Manager
|ISSUE|Summary|
|:--|:--|
|[INLONG-10703](https://github.com/apache/inlong/issues/10703)|[Feature][Manager] Manager should support oceanbase|
|[INLONG-10723](https://github.com/apache/inlong/issues/10723)|[Improve][Manager] Update base64 encoder|
|[INLONG-10758](https://github.com/apache/inlong/issues/10758)|[Improve][Manager] Support obtaining serialization configuration when wrapType is raw|
|[INLONG-10834](https://github.com/apache/inlong/issues/10834)|[Improve][Manager] Standalone configuration supports Tube MQ|
|[INLONG-10856](https://github.com/apache/inlong/issues/10856)|[Improve][Manager] Support agent installation logs|
|[INLONG-10884](https://github.com/apache/inlong/issues/10884)|[Improve][Manager] Support configuring HTTP type sink|
|[INLONG-10907](https://github.com/apache/inlong/issues/10907)|[Improve][Manager] Support reinstalling the installer|
|[INLONG-10911](https://github.com/apache/inlong/issues/10911)|[Improve][Manager] Support pagination to query sort task details information|
|[INLONG-10931](https://github.com/apache/inlong/issues/10931)|[Improve][Manager] Data preview supports data containing escape characters|
|[INLONG-10954](https://github.com/apache/inlong/issues/10954)|[Improve][Manager] Support fields of timestamptz type|
|[INLONG-10977](https://github.com/apache/inlong/issues/10977)|[Improve][Manager] Data preview supports escaping for KV data type |
|[INLONG-10988](https://github.com/apache/inlong/issues/10988)|[Improve][Manager] Data preview filters data in tubes based on streamId|
|[INLONG-11089](https://github.com/apache/inlong/issues/11089)|[Improve][Manager] Optimize Sort filter function|
|[INLONG-11091](https://github.com/apache/inlong/issues/11091)|[Improve][Manager] Manager supports in filter function configuration|
|[INLONG-11103](https://github.com/apache/inlong/issues/11103)|[Improve][Manager] Data add task supports filtering based on stream|
|[INLONG-11157](https://github.com/apache/inlong/issues/11157)|[Improve][Manager] Asynchronous processing agent installation|
|[INLONG-11195](https://github.com/apache/inlong/issues/11195)|[Improve][Manager] It is not allowed to modify group information when ordinary users are not responsible|
|[INLONG-11313](https://github.com/apache/inlong/issues/11313)|[Improve][Manager] Dataproxy cluster increases maximum packet length configuration|
|[INLONG-11323](https://github.com/apache/inlong/issues/11323)|[Improve][Manager] Modify the parameters of the data add tasks for file collection|
|[INLONG-11335](https://github.com/apache/inlong/issues/11335)|[Improve][Manager] Move maxPacketLength to the DataProxyNodeResponse|
|[INLONG-10730](https://github.com/apache/inlong/issues/10730)|[Bug][Manager] Auto-assign wrong Sortstandalone cluster when no cluster is under the tag|
|[INLONG-10754](https://github.com/apache/inlong/issues/10754)|[Bug][Manager] Offline data sync may create too many scheduling instances at the start point|
|[INLONG-10763](https://github.com/apache/inlong/issues/10763)|[Bug][Manager] Exception occurs when get or update offline sync group information |
|[INLONG-10842](https://github.com/apache/inlong/issues/10842)|[Bug][Manager] Tube cluster address not obtained when obtaining consumer group information|
|[INLONG-10918](https://github.com/apache/inlong/issues/10918)|[Bug][Manager] The correct command was not used when reinstalling the installer|
|[INLONG-10921](https://github.com/apache/inlong/issues/10921)|[Bug][Manager] Task configuration not deleted when deleting streamSource|
|[INLONG-10975](https://github.com/apache/inlong/issues/10975)|[Bug][Manager] When saving the group, only the existence of the groupid under the current tenant was verified|
|[INLONG-10997](https://github.com/apache/inlong/issues/10997)|[Bug][Manager] Incorrect setting of transformSQL in dataflowconfig|
|[INLONG-11071](https://github.com/apache/inlong/issues/11071)|[Bug][Manager] Failed to handle request on inlong_agent_system by admin |
|[INLONG-11095](https://github.com/apache/inlong/issues/11095)|[Bug][Manager]Data preview field misalignment|
|[INLONG-11142](https://github.com/apache/inlong/issues/11142)|[Bug][Manager] Data add task not scheduled for cleaning|
|[INLONG-11150](https://github.com/apache/inlong/issues/11150)|[Bug][Manager] Incorrect setting of sorTaskName for sink|
|[INLONG-11153](https://github.com/apache/inlong/issues/11153)|[Bug][Manager] HTTP sink does not automatically allocate sort cluster|
|[INLONG-11163](https://github.com/apache/inlong/issues/11163)|[Bug][Manager] Adding dataaddtask failed|
|[INLONG-11307](https://github.com/apache/inlong/issues/11307)|[Bug][Manager] Unable to obtain extparams from multiple data sources|

### SDK
|ISSUE|Summary|
|:--|:--|
|[INLONG-10119](https://github.com/apache/inlong/issues/10119)|[Feature][SDK] Supporting Data Sharding with GroupBy Semantics|
|[INLONG-10128](https://github.com/apache/inlong/issues/10128)|[Feature][SDK] Support to parse Map node in JSON or PB protocol data|
|[INLONG-10464](https://github.com/apache/inlong/issues/10464)|[Feature][SDK] InlongSDK support retry sending when failed|
|[INLONG-10613](https://github.com/apache/inlong/issues/10613)|[Feature][SDK] Transform SQL support arithmetic functions(Including ceil, floor, sin and sinh)|
|[INLONG-10618](https://github.com/apache/inlong/issues/10618)|[Feature][SDK] Transform SQL support common functions(Including substring, locate, to_date and date_format)|
|[INLONG-10764](https://github.com/apache/inlong/issues/10764)|[Feature][SDK] Transform SQL support temporal functions(Including year, quarter, month, week, dayofyear and dayofmonth)|
|[INLONG-10767](https://github.com/apache/inlong/issues/10767)|[Feature][SDK] Transform SQL support temporal functions(Including hour, minute and second)|
|[INLONG-10781](https://github.com/apache/inlong/issues/10781)|[Feature][SDK] Transform SQL support temporal functions(Including form_unixtime, unix_timestamp and to_timestamp)|
|[INLONG-10803](https://github.com/apache/inlong/issues/10803)|[Feature][SDK] Transform SQL support Round function |
|[INLONG-10813](https://github.com/apache/inlong/issues/10813)|[Feature][SDK] Transform support cos function|
|[INLONG-10816](https://github.com/apache/inlong/issues/10816)|[Feature][SDK] Transform support Replace function|
|[INLONG-10819](https://github.com/apache/inlong/issues/10819)|[Feature][SDK] Transform support tan function|
|[INLONG-10823](https://github.com/apache/inlong/issues/10823)|[Feature][SDK] Transform SQL support Reverse function|
|[INLONG-10826](https://github.com/apache/inlong/issues/10826)|[Feature][SDK] Transform support TRIM(), REPLICATE() function|
|[INLONG-10833](https://github.com/apache/inlong/issues/10833)|[Feature][SDK] Transform SQL supports parsing of Mod methods and % expressions|
|[INLONG-10841](https://github.com/apache/inlong/issues/10841)|[Feature][SDK] Transform SQL supports toBase64 function|
|[INLONG-10859](https://github.com/apache/inlong/issues/10859)|[Feature][SDK] Transform support factorial function|
|[INLONG-10864](https://github.com/apache/inlong/issues/10864)|[Feature][SDK] Transform SQL support md5 function|
|[INLONG-10866](https://github.com/apache/inlong/issues/10866)|[Feature][SDK] Transform SQL support sign function|
|[INLONG-10867](https://github.com/apache/inlong/issues/10867)|[Feature][SDK] Transform support BETWEEN AND operator|
|[INLONG-10869](https://github.com/apache/inlong/issues/10869)|[Feature][SDK] Transform SQL supports case conversion of strings|
|[INLONG-10876](https://github.com/apache/inlong/issues/10876)|[Feature][SDK] Transform support RAND() function|
|[INLONG-10879](https://github.com/apache/inlong/issues/10879)|[Feature][SDK] Transform support TIMESTAMPADD() function|
|[INLONG-10881](https://github.com/apache/inlong/issues/10881)|[Feature][SDK] Transform SQL support Concat_ws function|
|[INLONG-10882](https://github.com/apache/inlong/issues/10882)|[Feature][SDK] Transform SQL support ASCII function|
|[INLONG-10883](https://github.com/apache/inlong/issues/10883)|[Feature][SDK] Transform SQL support InitCap function|
|[INLONG-10886](https://github.com/apache/inlong/issues/10886)|[Feature][SDK] Transform support ACOS function|
|[INLONG-10893](https://github.com/apache/inlong/issues/10893)|[Feature][SDK] Transform SQL support FromBase64 function|
|[INLONG-10897](https://github.com/apache/inlong/issues/10897)|[Feature][SDK] Transform support DATEDIFF function|
|[INLONG-10901](https://github.com/apache/inlong/issues/10901)|[Feature][SDK] Transform support BIN(integer) function|
|[INLONG-10902](https://github.com/apache/inlong/issues/10902)|[Feature][SDK] Transform support HEX(numeric or string) function|
|[INLONG-10905](https://github.com/apache/inlong/issues/10905)|[Feature][SDK] Transform support Length() function|
|[INLONG-10906](https://github.com/apache/inlong/issues/10906)|[Feature][SDK] Transform supports the truncation of left and right strings|
|[INLONG-10920](https://github.com/apache/inlong/issues/10920)|[Feature][SDK] Transform support LocalTime function|
|[INLONG-10927](https://github.com/apache/inlong/issues/10927)|[Feature][SDK] Transform supports padding of left and right strings|
|[INLONG-10929](https://github.com/apache/inlong/issues/10929)|[Feature][SDK] Support Inlong Transform function annotation|
|[INLONG-10930](https://github.com/apache/inlong/issues/10930)|[Feature][SDK] Transform support DAYOFWEEK function|
|[INLONG-10934](https://github.com/apache/inlong/issues/10934)|[Feature][SDK] Transform support LocalDate function|
|[INLONG-10935](https://github.com/apache/inlong/issues/10935)|[Feature][SDK] Transform support DAYNAME function|
|[INLONG-10938](https://github.com/apache/inlong/issues/10938)|[Feature][SDK] Transform SQL supports fuzzy matching of LIKE and NOT LIKE|
|[INLONG-10939](https://github.com/apache/inlong/issues/10939)|[Feature][SDK] Transform SQL supports STRCMP function|
|[INLONG-10940](https://github.com/apache/inlong/issues/10940)|[Feature][SDK] Transform SQL support arithmetic functions(Including cot, tanh, cosh, asin, atan and atan2)|
|[INLONG-10942](https://github.com/apache/inlong/issues/10942)|[Feature][SDK] Add official function names for all Transform functions|
|[INLONG-10944](https://github.com/apache/inlong/issues/10944)|[Feature][SDK] Support Inlong Transform parser annotation|
|[INLONG-10946](https://github.com/apache/inlong/issues/10946)|[Feature][SDK] Transform SQL supports SPACE function|
|[INLONG-10952](https://github.com/apache/inlong/issues/10952)|[Feature][SDK] Transform SQL supports DATE_SUB function|
|[INLONG-10953](https://github.com/apache/inlong/issues/10953)|[Feature][SDK] Transform SQL supports DATE_ADD function |
|[INLONG-10956](https://github.com/apache/inlong/issues/10956)|[Feature][SDK] Transform SQL supports SHA encryption algorithm|
|[INLONG-10959](https://github.com/apache/inlong/issues/10959)|[Feature][SDK] Transform support IFNULL function|
|[INLONG-10962](https://github.com/apache/inlong/issues/10962)|[Feature][SDK] Transform JSON Source Support Multi-Dimensional Array|
|[INLONG-10963](https://github.com/apache/inlong/issues/10963)|[Feature][SDK] Transform SQL support CONTAINS function.|
|[INLONG-10965](https://github.com/apache/inlong/issues/10965)|[Feature][SDK] Transform support Bitwise operation|
|[INLONG-10966](https://github.com/apache/inlong/issues/10966)|[Feature][SDK] Fix HexFunction set the Java keyword 'public'|
|[INLONG-10968](https://github.com/apache/inlong/issues/10968)|[Feature][SDK] Support Inlong Transform operator annotation|
|[INLONG-10971](https://github.com/apache/inlong/issues/10971)|[Feature][SDK] Transform support INSERT function|
|[INLONG-10979](https://github.com/apache/inlong/issues/10979)|[Feature][SDK] Transform support PI() function|
|[INLONG-10980](https://github.com/apache/inlong/issues/10980)|[Feature][SDK] Transform support STRCMP(str1, str2) function|
|[INLONG-10984](https://github.com/apache/inlong/issues/10984)|[Feature][SDK] Transform support RADIANS(x) function|
|[INLONG-10986](https://github.com/apache/inlong/issues/10986)|[Feature][SDK] Transform support REGEXP_MATCHES(str, pattern) function|
|[INLONG-10991](https://github.com/apache/inlong/issues/10991)|[Feature][SDK] Transform SQL supports SHA encryption algorithm|
|[INLONG-10995](https://github.com/apache/inlong/issues/10995)|[Feature][SDK] Transform SQL support Chr function|
|[INLONG-10999](https://github.com/apache/inlong/issues/10999)|[Feature][SDK] Support to return raw data by star sign in transformer SQL|
|[INLONG-11000](https://github.com/apache/inlong/issues/11000)|[Feature][SDK] Add XML formatted data source for Transform|
|[INLONG-11005](https://github.com/apache/inlong/issues/11005)|[Feature][SDK] Add YAML formatted data source for Transform|
|[INLONG-11007](https://github.com/apache/inlong/issues/11007)|[Feature][SDK] Transform SQL support INSTR function. |
|[INLONG-11008](https://github.com/apache/inlong/issues/11008)|[Feature][SDK] Transform SQL support PRINTF function.|
|[INLONG-11010](https://github.com/apache/inlong/issues/11010)|[Feature][SDK] Transform refactor unit test structure|
|[INLONG-11012](https://github.com/apache/inlong/issues/11012)|[Feature][SDK] Transform SQL supports LCASE and UCASE Functions|
|[INLONG-11017](https://github.com/apache/inlong/issues/11017)|[Feature][SDK] Transform SQL supports RTRIM and LTRIM Functions|
|[INLONG-11018](https://github.com/apache/inlong/issues/11018)|[Feature][SDK] Transform SQL supports COMPRESS and UNCOMPRESS Functions|
|[INLONG-11019](https://github.com/apache/inlong/issues/11019)|[Feature][SDK] Optimize the mapping strategy of TransformSQL select field list, allowing the order of select field list and sink field list to be inconsistent|
|[INLONG-11020](https://github.com/apache/inlong/issues/11020)|[Feature][SDK] Add BSON formatted data source for Transform|
|[INLONG-11022](https://github.com/apache/inlong/issues/11022)|[Feature][SDK] Transform SQL support ParseUrl function|
|[INLONG-11025](https://github.com/apache/inlong/issues/11025)|[Feature][SDK] Delete the integration test class for the test cases of Transform|
|[INLONG-11028](https://github.com/apache/inlong/issues/11028)|[Feature][SDK] Transform SQL support UrlEncode & UrlDecode functions|
|[INLONG-11029](https://github.com/apache/inlong/issues/11029)|[Feature][SDK] Transform SQL support StartsWith & EndsWith functions|
|[INLONG-11030](https://github.com/apache/inlong/issues/11030)| [Feature][SDK] Add AVRO formatted data source for Transform|
|[INLONG-11033](https://github.com/apache/inlong/issues/11033)|[Feature][SDK] Transform support TRANSLATE(string text, from text, to text) function|
|[INLONG-11034](https://github.com/apache/inlong/issues/11034)|[Feature][SDK] Transform support UUID() function|
|[INLONG-11035](https://github.com/apache/inlong/issues/11035)|[Feature][SDK] Transform support TRUNCATE(numeric1, integer2) function|
|[INLONG-11036](https://github.com/apache/inlong/issues/11036)|[Feature][SDK] Transform update ReplicateFunction|
|[INLONG-11037](https://github.com/apache/inlong/issues/11037)|[Feature][SDK] Transform support ENCODE() and DECODE() function|
|[INLONG-11042](https://github.com/apache/inlong/issues/11042)|[Feature][SDK] Transform refactor function package|
|[INLONG-11044](https://github.com/apache/inlong/issues/11044)|[Feature][SDK] Transform SQL supports string judgment functions (IS_ALPHA、IS_DECIMAL、IS_DIGIT)|
|[INLONG-11045](https://github.com/apache/inlong/issues/11045)|[Feature][SDK] Transform SQL supports the conversion between characters and ASCII|
|[INLONG-11046](https://github.com/apache/inlong/issues/11046)|[Feature][SDK] Transform SQL supports STARTSWITH and ENDSWITH Functions|
|[INLONG-11047](https://github.com/apache/inlong/issues/11047)|[Feature][SDK] Add NP check and parameter judgment to the Replicate function|
|[INLONG-11049](https://github.com/apache/inlong/issues/11049)|[Feature][SDK] Standardize existing functions|
|[INLONG-11050](https://github.com/apache/inlong/issues/11050)|[Feature][SDK] Transform SQL supports bit_length Function|
|[INLONG-11053](https://github.com/apache/inlong/issues/11053)|[Feature][SDK] Transform support SPLIT_INDEX function|
|[INLONG-11056](https://github.com/apache/inlong/issues/11056)|[Feature][SDK] Transform support RAND_INTEGER() function|
|[INLONG-11057](https://github.com/apache/inlong/issues/11057)|[Feature][SDK] Transform support E() function|
|[INLONG-11058](https://github.com/apache/inlong/issues/11058)|[Feature][SDK] Transform support REGEXP_REPLACE() function|
|[INLONG-11059](https://github.com/apache/inlong/issues/11059)|[Feature][SDK] Transform support REGEXP() function|
|[INLONG-11060](https://github.com/apache/inlong/issues/11060)|[Feature][SDK] Transform support REGEXP_...() related functions|
|[INLONG-11064](https://github.com/apache/inlong/issues/11064)|[Feature][SDK] Transform SQL supports NULLIF function|
|[INLONG-11079](https://github.com/apache/inlong/issues/11079)|[Feature][SDK] Transform SQL supports DATE_ADD function|
|[INLONG-11080](https://github.com/apache/inlong/issues/11080)|[Feature][SDK] Transform SQL supports DATE_SUB function|
|[INLONG-11081](https://github.com/apache/inlong/issues/11081)|[Feature][SDK] Transform SQL supports INTERVAL parse|
|[INLONG-11092](https://github.com/apache/inlong/issues/11092)|[Feature][SDK] Fix the issue of inconsistent capitalization of Function names in FunctionalTools|
|[INLONG-11107](https://github.com/apache/inlong/issues/11107)|[Feature][SDK] Avro Source Data Support Map Type|
|[INLONG-11109](https://github.com/apache/inlong/issues/11109)|[Feature][SDK] Transform SQL supports parsing of IS|
|[INLONG-11112](https://github.com/apache/inlong/issues/11112)|[Feature][SDK] Transform TRUNCATE() function add pgsql name|
|[INLONG-11114](https://github.com/apache/inlong/issues/11114)|[Feature][SDK] Transform support Trigonometric function in units of degrees|
|[INLONG-11115](https://github.com/apache/inlong/issues/11115)|[Feature][SDK] Transform support computes different types hash|
|[INLONG-11117](https://github.com/apache/inlong/issues/11117)|[Feature][SDK] Transform SQL supports ISNULL functions|
|[INLONG-11121](https://github.com/apache/inlong/issues/11121)|[Feature][SDK] Transform SQL supports gcd functions|
|[INLONG-11122](https://github.com/apache/inlong/issues/11122)|[Feature][SDK] Transform SQL supports UNHEX function|
|[INLONG-11123](https://github.com/apache/inlong/issues/11123)|[Feature][SDK] Transform SQL supports conv function|
|[INLONG-11124](https://github.com/apache/inlong/issues/11124)|[Feature][SDK] Transform fix ENCODE() and DECODE() function|
|[INLONG-11125](https://github.com/apache/inlong/issues/11125)|[Feature][SDK] Transform OperatorTools support parseBytes|
|[INLONG-11132](https://github.com/apache/inlong/issues/11132)|[Feature][SDK] Transform SQL support parsing SIMILAR TO|
|[INLONG-11134](https://github.com/apache/inlong/issues/11134)|[Feature][SDK] Add Parquet formatted data source for Transform|
|[INLONG-11138](https://github.com/apache/inlong/issues/11138)|[Feature][SDK] Add ORC formatted data source for Transform|
|[INLONG-11139](https://github.com/apache/inlong/issues/11139)|[Feature][SDK] Transform support Not Between And operator|
|[INLONG-11147](https://github.com/apache/inlong/issues/11147)|[Feature][SDK] Transform support CASE Operator|
|[INLONG-11152](https://github.com/apache/inlong/issues/11152)|[Feature][SDK] Transform SQL supports TIMESTAMPDIFF function|
|[INLONG-11154](https://github.com/apache/inlong/issues/11154)|[Feature][SDK] Transform SQL supports TIMESTAMP function|
|[INLONG-11167](https://github.com/apache/inlong/issues/11167)|[Feature][SDK] Transform REGEXP function add usage of regexp_like|
|[INLONG-11168](https://github.com/apache/inlong/issues/11168)|[Feature][SDK] Transform support REGEXP_SPLIT_TO_ARRAY() function|
|[INLONG-11172](https://github.com/apache/inlong/issues/11172)|[Feature][SDK] Fix bug in Transform REGEXP_MATCHES() function|
|[INLONG-11192](https://github.com/apache/inlong/issues/11192)|[Feature][SDK] Transform SQL supports TIMEDIFF function|
|[INLONG-11207](https://github.com/apache/inlong/issues/11207)|[Feature][SDK] Transform support IN and NOT IN expression|
|[INLONG-11208](https://github.com/apache/inlong/issues/11208)|[Feature][SDK] Transform support EXISTS() operator|
|[INLONG-11209](https://github.com/apache/inlong/issues/11209)|[Feature][SDK] Transform TRIM() function add usage of BTRIM()|
|[INLONG-11216](https://github.com/apache/inlong/issues/11216)|[Feature][SDK] Transform support STR_TO_MAP() function|
|[INLONG-11217](https://github.com/apache/inlong/issues/11217)|[Feature][SDK] Transform support JSON_QUOTE() and JSON_UNQUOTE() function|
|[INLONG-11218](https://github.com/apache/inlong/issues/11218)|[Feature][SDK] Transform support CONVERT_TZ() function|
|[INLONG-11219](https://github.com/apache/inlong/issues/11219)|[Feature][SDK] Transform support COALESCE() function|
|[INLONG-11220](https://github.com/apache/inlong/issues/11220)|[Feature][SDK] Transform support GREATEST() and LEAST() function|
|[INLONG-11221](https://github.com/apache/inlong/issues/11221)|[Feature][SDK] Transform support JSON_EXISTS() function|
|[INLONG-11222](https://github.com/apache/inlong/issues/11222)|[Feature][SDK] Transform support JSON_STRING() function|
|[INLONG-11223](https://github.com/apache/inlong/issues/11223)|[Feature][SDK] Transform support JSON_VALUE() function|
|[INLONG-11224](https://github.com/apache/inlong/issues/11224)|[Feature][SDK] Transform support JSON_QUERY() function|
|[INLONG-11225](https://github.com/apache/inlong/issues/11225)|[Feature][SDK] Transform support JSON_ARRAY() function|
|[INLONG-11226](https://github.com/apache/inlong/issues/11226)|[Feature][SDK] Transform support parse IS DISTINCT FROM|
|[INLONG-11227](https://github.com/apache/inlong/issues/11227)|[Feature][SDK] Add Parquet formatted data sink for Transform|
|[INLONG-11229](https://github.com/apache/inlong/issues/11229)|[Feature][SDK] Transform Support ELT Function|
|[INLONG-11230](https://github.com/apache/inlong/issues/11230)|[Feature][SDK] Transform Support TO_TIMESTAMP_LTZ Function|
|[INLONG-11233](https://github.com/apache/inlong/issues/11233)|[Feature][SDK] Transform SQL supports mid function|
|[INLONG-11235](https://github.com/apache/inlong/issues/11235)|[Feature][SDK] Transform SQL supports SUBSTRING_INDEX function|
|[INLONG-11236](https://github.com/apache/inlong/issues/11236)|[Feature][SDK] Transform SQL supports FIND_IN_SET function|
|[INLONG-11237](https://github.com/apache/inlong/issues/11237)|[Feature][SDK] Transform SQL supports CHAR_LENGTH function|
|[INLONG-11238](https://github.com/apache/inlong/issues/11238)|[Feature][SDK] Transform SQL supports FORMAT function|
|[INLONG-11239](https://github.com/apache/inlong/issues/11239)|[Feature][SDK] Transform SQL supports INSTR function|
|[INLONG-11240](https://github.com/apache/inlong/issues/11240)|[Feature][SDK] Transform LENGTH() function add flinksql usage|
|[INLONG-11241](https://github.com/apache/inlong/issues/11241)|[Feature][SDK] Transform support ARRAY() function|
|[INLONG-11242](https://github.com/apache/inlong/issues/11242)|[Feature][SDK] Transform support MAP() function|
|[INLONG-11252](https://github.com/apache/inlong/issues/11252)|[Feature][SDK] Transform support PB sink data|
|[INLONG-11259](https://github.com/apache/inlong/issues/11259)|[Feature][SDK] Transform SQL supports "IS [NOT] DISTINCT" parsing|
|[INLONG-11260](https://github.com/apache/inlong/issues/11260)|[Feature][SDK] Transform SQL supports "num_nonnulls" and "num_nulls" function|
|[INLONG-11261](https://github.com/apache/inlong/issues/11261)|[Feature][SDK] Transform SQL supports "cbrt" function|
|[INLONG-11262](https://github.com/apache/inlong/issues/11262)|[Feature][SDK] Transform SQL supports "erf" and "erfc" function|
|[INLONG-11263](https://github.com/apache/inlong/issues/11263)|[Feature][SDK] Transform SQL supports "lcm" function|
|[INLONG-11264](https://github.com/apache/inlong/issues/11264)|[Feature][SDK] Transform SQL supports functions related to scale|
|[INLONG-11265](https://github.com/apache/inlong/issues/11265)|[Feature][SDK] Transform SQL supports "trunc" function|
|[INLONG-11282](https://github.com/apache/inlong/issues/11282)|[Feature][SDK] Transform enhance Array-related Collection Functions|
|[INLONG-11283](https://github.com/apache/inlong/issues/11283)|[Feature][SDK]Transform enhance Map-related Collection Functions|
|[INLONG-11300](https://github.com/apache/inlong/issues/11300)|[Feature][SDK] Transform SQL supports "JSON_ARRAY_APPEND" function|
|[INLONG-11301](https://github.com/apache/inlong/issues/11301)|[Feature][SDK] Transform SQL supports "JSON_ARRAY_INSERT" function|
|[INLONG-11337](https://github.com/apache/inlong/issues/11337)|[Feature][SDK] Transform refactor InitCap functions' package|
|[INLONG-10463](https://github.com/apache/inlong/issues/10463)|[Improve][SDK] Optimization of ultra-long field processing in InlongSDK|
|[INLONG-10652](https://github.com/apache/inlong/issues/10652)|[Improve][SDK] Inlong Transform support for generics|
|[INLONG-10684](https://github.com/apache/inlong/issues/10684)|[Improve][SDK] Inlong transform supports context|
|[INLONG-10706](https://github.com/apache/inlong/issues/10706)|[Improve][SDK]Shaded Native Library to reduce conflicts with other sdk|
|[INLONG-10736](https://github.com/apache/inlong/issues/10736)|[Improve][SDK] Python dataproxy sdk build script support for skipping c++ dataproxy sdk build step|
|[INLONG-10774](https://github.com/apache/inlong/issues/10774)|[Improve][SDK] Optimize Cmake compilation script for CPP DataProxy SDK|
|[INLONG-10780](https://github.com/apache/inlong/issues/10780)|[Improve][SDK] Optimize memory management for DataProxy CPP SDK|
|[INLONG-10789](https://github.com/apache/inlong/issues/10789)|[Improve][SDK] Update build script for python dataproxy sdk|
|[INLONG-10793](https://github.com/apache/inlong/issues/10793)|[Improve][SDK] Added metric management for DataProxy CPP SDK|
|[INLONG-10806](https://github.com/apache/inlong/issues/10806)|[Improve][SDK] Optimize python dataproxy sdk build script|
|[INLONG-10809](https://github.com/apache/inlong/issues/10809)|[Improve][SDK] Improvements to TypeConverter field types and CompareValue in OperatorTools|
|[INLONG-10821](https://github.com/apache/inlong/issues/10821)|[Improve][SDK] Optimize the ability to receive data for DataProxy C++ SDK|
|[INLONG-10822](https://github.com/apache/inlong/issues/10822)|[Improve][SDK] Add message manager for DataProxy C++ SDK|
|[INLONG-10838](https://github.com/apache/inlong/issues/10838)|[Improve][SDK] Optimize the ability to send data for DataProxy C++ SDK|
|[INLONG-10851](https://github.com/apache/inlong/issues/10851)|[Improve][SDK]  Support multiple protocols for DataProxy C++ SDK|
|[INLONG-10861](https://github.com/apache/inlong/issues/10861)|[Improve][SDK] Optimize the coredump caused by the DataProxy C++ SDK|
|[INLONG-10872](https://github.com/apache/inlong/issues/10872)|[Improve][SDK] Enhance Test Coverage with Additional Complex Json Test Cases|
|[INLONG-10894](https://github.com/apache/inlong/issues/10894)|[Improve][SDK] Remove useless interfaces exposed in pybind binding code|
|[INLONG-10913](https://github.com/apache/inlong/issues/10913)|[Improve][SDK] Add requirements.txt and auto delete pybind directory when build failed in python sdk |
|[INLONG-10957](https://github.com/apache/inlong/issues/10957)|[Improve][SDK] Improve some code structures|
|[INLONG-10982](https://github.com/apache/inlong/issues/10982)|[Improve][SDK] Improve the test code structure|
|[INLONG-11074](https://github.com/apache/inlong/issues/11074)|[Improve][SDK] Transform support dummy select when the transform sql is empty|
|[INLONG-11087](https://github.com/apache/inlong/issues/11087)|[Improve][SDK] Fix potential null pointer exception of transform SDK|
|[INLONG-11097](https://github.com/apache/inlong/issues/11097)|[Improve][SDK] Optimize logs of the transform SDK|
|[INLONG-11156](https://github.com/apache/inlong/issues/11156)|[Improve][SDK] SortSDK support that the token configuration of pulsar cluster is null|
|[INLONG-11190](https://github.com/apache/inlong/issues/11190)|[Improve][SDK] Optimize Transform SDK Translate Function|
|[INLONG-11202](https://github.com/apache/inlong/issues/11202)|[Improve][SDK] Optimize the code related to the date type in Transform |
|[INLONG-11214](https://github.com/apache/inlong/issues/11214)|[Improve][SDK] Modify the problem of incomplete division in DivisionParser|
|[INLONG-11243](https://github.com/apache/inlong/issues/11243)|[Improve][SDK] Enhance the functionality of the substring function in Transform SQL|
|[INLONG-11325](https://github.com/apache/inlong/issues/11325)|[Improve][SDK] DataProxy SDK updates metadata causing temporary unavailability of sent data|
|[INLONG-10776](https://github.com/apache/inlong/issues/10776)|[Bug][SDK] The parsing type of the first parameter(timestamp) in DateFormatFunction is not correct|
|[INLONG-10811](https://github.com/apache/inlong/issues/10811)|[Bug][SDK] Python dataproxy sdk callback function call leads to coredump|
|[INLONG-11105](https://github.com/apache/inlong/issues/11105)|[Bug][SDK] The empty string is converted into a "null"|
|[INLONG-11110](https://github.com/apache/inlong/issues/11110)|[Bug][SDK] Fix incorrect usage of isLocalVisit variable in client example of dataproxy-sdk module|
|[INLONG-11161](https://github.com/apache/inlong/issues/11161)|[Bug][SDK] Fix TransformSDK UT bug of TestAdditionParser and TestSubtractionParser|

### Sort
|ISSUE|Summary|
|:--|:--|
|[INLONG-7056](https://github.com/apache/inlong/issues/7056)|[Feature][Sort] Adjust sort resources according to data scale|
|[INLONG-9487](https://github.com/apache/inlong/issues/9487)|[Feature][Sort] Pulsar connector support process time meta field |
|[INLONG-10644](https://github.com/apache/inlong/issues/10644)|[Feature][Sort] The base common of elasticsearch connector is neccessary.|
|[INLONG-10704](https://github.com/apache/inlong/issues/10704)|[Feature][Sort] sort should support oceanbase |
|[INLONG-10720](https://github.com/apache/inlong/issues/10720)|[Feature][Sort] Add Elasticsearch6 connector on Flink 1.18|
|[INLONG-10721](https://github.com/apache/inlong/issues/10721)|[Feature][Sort] Add Elasticsearch7 connector on Flink 1.18|
|[INLONG-10729](https://github.com/apache/inlong/issues/10729)|[Feature][Sort] Sorstandalone EsSink support transform|
|[INLONG-10739](https://github.com/apache/inlong/issues/10739)|[Feature][Sort]The base common of elasticsearch connector on flink 1.15 is neccessary|
|[INLONG-10768](https://github.com/apache/inlong/issues/10768)|[Feature][Sort] Csv utils support specified the max split field size|
|[INLONG-10791](https://github.com/apache/inlong/issues/10791)|[Feature][Sort] Support inlong dirty sink|
|[INLONG-10828](https://github.com/apache/inlong/issues/10828)|[Feature][Sort] Add config class of SortStandalone to support HTTP report|
|[INLONG-10831](https://github.com/apache/inlong/issues/10831)|[Feature][Sort] SortStandalone support HTTP sink|
|[INLONG-11065](https://github.com/apache/inlong/issues/11065)|[Feature][Sort] Provides a method to add openTelemetryAppender for the sort connector|
|[INLONG-11129](https://github.com/apache/inlong/issues/11129)|[Feature][Sort] Enhanced source metric instrumentation for InLong Sort Flink Connector|
|[INLONG-11188](https://github.com/apache/inlong/issues/11188)|[Feature][Sort]  HTTP SortStandalone supports batch sorting capability|
|[INLONG-11199](https://github.com/apache/inlong/issues/11199)|[Feature][Sort] Integrate log management systems for connectors|
|[INLONG-11201](https://github.com/apache/inlong/issues/11201)|[Feature][Sort] Enhanced sink metric instrumentation for InLong Sort Flink Connector|
|[INLONG-10773](https://github.com/apache/inlong/issues/10773)|[Improve][Sort] The format-base module can't compile on flink 1.18 env|
|[INLONG-10784](https://github.com/apache/inlong/issues/10784)|[Improve][Sort] The format-row module and format-rowdata have same package structure.|
|[INLONG-11068](https://github.com/apache/inlong/issues/11068)|[Improve][Sort] Optimize kafka producer parameters|
|[INLONG-11072](https://github.com/apache/inlong/issues/11072)|[Improve][Sort] Optimize EsSink when transform processor is null|
|[INLONG-11076](https://github.com/apache/inlong/issues/11076)|[Improve][Sort] Discard unretryable exception when send kafka failed|
|[INLONG-11266](https://github.com/apache/inlong/issues/11266)|[Improve][Sort] Add end-to-end test case for sort-connector-pulsar-v1.15|
|[INLONG-10719](https://github.com/apache/inlong/issues/10719)|[Bug]During the execution of the InLong Sort-Oracle-CDC task, the Extract node generates a large number of archive logs, and the Load node fails to capture incremental change data.|
|[INLONG-11100](https://github.com/apache/inlong/issues/11100)|[Bug][Sort] The buffer queue is not released after sending messages to elasticsearch|
|[INLONG-11148](https://github.com/apache/inlong/issues/11148)|[Bug][Sort] Log4j2 configuration of sort-end-to-end-test-v1.15 not working properly|
|[INLONG-11166](https://github.com/apache/inlong/issues/11166)|[Bug][Sort] Mongodb2StarRocksTest failure due to potential dependency conflicts|

### Audit
|ISSUE|Summary|
|:--|:--|
|[INLONG-10728](https://github.com/apache/inlong/issues/10728)|[Improve][Audit]  Add global memory control for the Audit SDK|
|[INLONG-10745](https://github.com/apache/inlong/issues/10745)|[Improve][Audit]  Unify the range of audit aggregation intervals|
|[INLONG-10799](https://github.com/apache/inlong/issues/10799)|[Improve][Audit] Support fork child process for DataProxy CPP SDK|
|[INLONG-11286](https://github.com/apache/inlong/issues/11286)|[Improve][Audit] Optimize the statistics of daily Audit data|
|[INLONG-11315](https://github.com/apache/inlong/issues/11315)|[Improve][Audit] Resolve the conflict between the Audit SDK and other components' Protobuf versions|
|[INLONG-11330](https://github.com/apache/inlong/issues/11330)|[Improve][Audit] Audit SDK supports custom setting of local IP|

