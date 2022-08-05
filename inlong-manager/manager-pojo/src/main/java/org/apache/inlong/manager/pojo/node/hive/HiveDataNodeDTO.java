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

package org.apache.inlong.manager.pojo.node.hive;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.DataSeparator;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.FileFormat;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.AESUtils;
import org.apache.inlong.manager.pojo.sink.hive.HivePartitionField;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * hive data node info
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Hive data node info")
public class HiveDataNodeDTO {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(); // thread safe

    @ApiModelProperty("Hive JDBC URL, such as jdbc:hive2://${ip}:${port}")
    private String jdbcUrl;

    @ApiModelProperty("Username of the Hive server")
    private String username;

    @ApiModelProperty("User password of the Hive server")
    private String password;

    @NotBlank(message = "dbName cannot be blank")
    @ApiModelProperty("Target database name")
    private String dbName;

    @NotBlank(message = "tableName cannot be blank")
    @ApiModelProperty("Target table name")
    private String tableName;

    @NotBlank(message = "dataPath cannot be blank")
    @ApiModelProperty("Data path, such as: hdfs://ip:port/user/hive/warehouse/test.db")
    private String dataPath;

    @ApiModelProperty("Partition interval, support: 1 H, 1 D, 30 I, 10 I")
    private Integer partitionInterval;

    @ApiModelProperty("Partition field list")
    private List<HivePartitionField> partitionFieldList;

    @ApiModelProperty("Partition creation strategy, partition start, partition close")
    private String partitionCreationStrategy;

    @ApiModelProperty("File format, support: TextFile, ORCFile, RCFile, SequenceFile, Avro, Parquet, etc")
    private String fileFormat = FileFormat.TextFile.name();

    @ApiModelProperty("Data encoding format: UTF-8, GBK")
    private String dataEncoding = StandardCharsets.UTF_8.toString();

    @ApiModelProperty("Data separator, stored as ASCII code")
    private String dataSeparator = DataSeparator.SOH.getSeparator();

    @ApiModelProperty("Version for Hive, such as: 3.2.1")
    private String hiveVersion;

    @ApiModelProperty("Config directory of Hive on HDFS, needed by sort in light mode, must include hive-site.xml")
    private String hiveConfDir;

    @ApiModelProperty("Password encrypt version")
    private Integer encryptVersion;

    /**
     * Get the dto instance from the request
     */
    public static HiveDataNodeDTO getFromRequest(HiveDataNodeRequest request) throws Exception {

        Integer encryptVersion = AESUtils.getCurrentVersion(null);
        String passwd = null;
        if (StringUtils.isNotEmpty(request.getPassword())) {
            passwd = AESUtils.encryptToString(request.getPassword().getBytes(StandardCharsets.UTF_8),
                    encryptVersion);
        }
        return HiveDataNodeDTO.builder()
                .jdbcUrl(request.getJdbcUrl())
                .username(request.getUsername())
                .password(passwd)
                .dbName(request.getDbName())
                .tableName(request.getTableName())
                .dataPath(request.getDataPath())
                .partitionInterval(request.getPartitionInterval())
                .partitionCreationStrategy(request.getPartitionCreationStrategy())
                .fileFormat(request.getFileFormat())
                .dataEncoding(request.getDataEncoding())
                .hiveVersion(request.getHiveVersion())
                .hiveConfDir(request.getHiveConfDir())
                .encryptVersion(encryptVersion)
                .build();
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static HiveDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return OBJECT_MAPPER.readValue(extParams, HiveDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT.getMessage());
        }
    }

    private HiveDataNodeDTO decryptPassword() throws Exception {
        if (StringUtils.isNotEmpty(this.password)) {
            byte[] passwordBytes = AESUtils.decryptAsString(this.password, this.encryptVersion);
            this.password = new String(passwordBytes, StandardCharsets.UTF_8);
        }
        return this;
    }
}
