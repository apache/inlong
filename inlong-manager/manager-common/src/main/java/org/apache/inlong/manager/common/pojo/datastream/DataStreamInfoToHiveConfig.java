/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.common.pojo.datastream;

import java.util.List;
import lombok.Data;

/**
 * Data stream info for Hive config
 */
@Data
public class DataStreamInfoToHiveConfig {

    private Integer id;

    private String jdbcUrl;

    private int status;

    private String inlongStreamId;

    private String description;

    private String inlongGroupId;

    private String defaultSelectors;

    private String partitionType;

    private String partitionFields;

    private String partitionFieldPosition;

    private Integer clusterId;

    private String dataType;

    private String fieldSplitter;

    private String clusterTag;

    private String encodingType;

    private String hiveAddr;

    private String creator;

    private String userName;

    private String password;

    private String compression;

    private String warehouseDir;

    private boolean setHadoopUgi;

    private String hadoopUgi;

    private Integer storagePeriod;

    private String fsDefaultName;

    private String dbName;

    private String tableName;

    private String primaryPartition;

    private String secondaryPartition;

    private String fileFormat;

    private String p;

    private String mt;

    private String usTaskId;

    private String qualifiedThreshold;

    private Integer hiveType;

    private String usageInterval;

    private String partitionStrategy;

    private List<DataStreamExtInfo> extList;

}