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

package org.apache.inlong.manager.common.pojo.datastorage;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.BizConstant;

/**
 * Interaction objects for Hive storage
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Interaction objects for Hive storage")
public class StorageHiveInfo extends BaseStorageInfo {

    private String storageType = BizConstant.STORAGE_TYPE_HIVE;

    @ApiModelProperty("Hive JDBC URL")
    private String jdbcUrl;

    @ApiModelProperty("username for JDBC URL")
    private String username;

    @ApiModelProperty("user password")
    private String password;

    @ApiModelProperty("target database name")
    private String dbName;

    @ApiModelProperty("target table name")
    private String tableName;

    @ApiModelProperty("primary partition field")
    private String primaryPartition;

    @ApiModelProperty("secondary partition field")
    private String secondaryPartition;

    @ApiModelProperty("partition type, like: H-hour, D-day, W-week, M-month, O-once, R-regulation")
    private String partitionType;

    @ApiModelProperty("file format, support: TextFile, RCFile, SequenceFile, Avro")
    private String fileFormat;

    @ApiModelProperty("field splitter")
    private String fieldSplitter;

    @ApiModelProperty("data encoding type")
    private String encodingType;

    @ApiModelProperty("HDFS defaultFS")
    private String hdfsDefaultFs;

    @ApiModelProperty("warehouse directory")
    private String warehouseDir;

    @ApiModelProperty("interval at which Sort collects data to Hive is 10M, 15M, 30M, 1H, 1D")
    private String usageInterval;

    @ApiModelProperty("backend operation log")
    private String optLog;

    @ApiModelProperty("hive table field list")
    private List<StorageHiveFieldInfo> hiveFieldList;

    @ApiModelProperty("other ext info list")
    private List<StorageExtInfo> extList;

}
