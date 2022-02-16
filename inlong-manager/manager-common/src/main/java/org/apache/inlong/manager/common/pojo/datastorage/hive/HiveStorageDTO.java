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

package org.apache.inlong.manager.common.pojo.datastorage.hive;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import javax.validation.constraints.NotNull;

/**
 * Hive storage info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HiveStorageDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

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

    /**
     * Get the dto instance from the request
     */
    public static HiveStorageDTO getFromRequest(HiveStorageRequest request) {
        return HiveStorageDTO.builder()
                .jdbcUrl(request.getJdbcUrl())
                .username(request.getUsername())
                .password(request.getPassword())
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .hdfsDefaultFs(request.getHdfsDefaultFs())
                .warehouseDir(request.getWarehouseDir())
                .partitionInterval(request.getPartitionInterval())
                .partitionUnit(request.getPartitionUnit())
                .primaryPartition(request.getPrimaryPartition())
                .secondaryPartition(request.getSecondaryPartition())
                .partitionCreationStrategy(request.getPartitionCreationStrategy())
                .fileFormat(request.getFileFormat())
                .dataEncoding(request.getDataEncoding())
                .dataSeparator(request.getDataSeparator())
                .build();
    }

    public static HiveStorageDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, HiveStorageDTO.class);
        } catch (Exception e) {
            throw new BusinessException(BizErrorCodeEnum.STORAGE_INFO_INCORRECT.getMessage());
        }
    }

}
