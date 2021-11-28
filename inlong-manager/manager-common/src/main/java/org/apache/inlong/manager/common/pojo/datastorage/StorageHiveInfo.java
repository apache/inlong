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

    private String storageType = BizConstant.STORAGE_HIVE;

    @ApiModelProperty("Hive JDBC URL")
    private String jdbcUrl;

    @ApiModelProperty("Username for JDBC URL")
    private String username;

    @ApiModelProperty("User password")
    private String password;

    @ApiModelProperty("Target database name")
    private String dbName;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("HDFS defaultFS")
    private String hdfsDefaultFs;

    @ApiModelProperty("Warehouse directory")
    private String warehouseDir;

    @ApiModelProperty("Partition interval, support: 1 H, 1 D, 30 I, 10 I")
    private Integer partitionInterval;

    @ApiModelProperty("Partition type, support: D-day, H-hour, I-minute")
    private String partitionUnit;

    @ApiModelProperty("Primary partition field")
    private String primaryPartition;

    @ApiModelProperty("Secondary partition field")
    private String secondaryPartition;

    @ApiModelProperty("Partition creation strategy, partition start, partition close")
    private String partitionCreationStrategy;

    @ApiModelProperty("File format, support: TextFile, RCFile, SequenceFile, Avro")
    private String fileFormat;

    @ApiModelProperty("Data encoding type")
    private String dataEncoding;

    @ApiModelProperty("Data field separator")
    private String dataSeparator;

    @ApiModelProperty("Data storage period in Hive, unit: Day")
    private Integer storagePeriod;

    @ApiModelProperty("Backend operation log")
    private String optLog;

    @ApiModelProperty("hive table field list")
    private List<StorageHiveFieldInfo> hiveFieldList;

    @ApiModelProperty("other ext info list")
    private List<StorageExtInfo> extList;

}
