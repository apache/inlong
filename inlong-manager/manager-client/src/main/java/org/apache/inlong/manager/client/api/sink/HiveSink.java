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

package org.apache.inlong.manager.client.api.sink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.client.api.DataFormat;
import org.apache.inlong.manager.client.api.DataSeparator;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.SinkType;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Hive sink configuration")
public class HiveSink extends StreamSink {

    @ApiModelProperty(value = "Sink type", required = true)
    private SinkType sinkType = SinkType.HIVE;

    @ApiModelProperty("Hive meta db URL, etc jdbc:hive2://${ip}:${port}")
    private String jdbcUrl;

    @ApiModelProperty("Authentication for hive")
    private DefaultAuthentication authentication;

    @ApiModelProperty("Target database name")
    private String dbName;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("HDFS defaultFS, etc hdfs://${ip}:${port}")
    private String hdfsDefaultFs;

    @ApiModelProperty("Warehouse directory, etc /usr/hive/warehouse")
    private String warehouseDir;

    @ApiModelProperty("Data encoding format: UTF-8, GBK")
    private Charset charset = StandardCharsets.UTF_8;

    @ApiModelProperty("Data separator, stored as ASCII code")
    private DataSeparator dataSeparator = DataSeparator.SOH;

    public enum FileFormat {
        TextFile, RCFile, SequenceFile, Avro;

        public static FileFormat forName(String name) {
            for (FileFormat value : values()) {
                if (value.name().equals(name)) {
                    return value;
                }
            }
            throw new IllegalArgumentException(String.format("Unsupport FileFormat:%s", name));
        }
    }

    @ApiModelProperty("File format, support: TextFile, RCFile, SequenceFile, Avro")
    private FileFormat fileFormat;

    @ApiModelProperty("Create table or not")
    private boolean needCreated;

    @ApiModelProperty("Primary partition field, default null")
    private String primaryPartition;

    @ApiModelProperty("Secondary partition field, default null")
    private String secondaryPartition;

    @ApiModelProperty("Field definitions for hive")
    private List<StreamField> streamFields;

    @ApiModelProperty("Other properties if need")
    private Map<String, String> properties;

    @ApiModelProperty("Data format type for stream sink")
    private DataFormat dataFormat;
}

