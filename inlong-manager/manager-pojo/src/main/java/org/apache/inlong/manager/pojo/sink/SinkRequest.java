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

package org.apache.inlong.manager.pojo.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Maps;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.pojo.sink.ck.ClickHouseSinkRequest;
import org.apache.inlong.manager.pojo.sink.dlciceberg.DLCIcebergSinkRequest;
import org.apache.inlong.manager.pojo.sink.es.ElasticsearchSinkRequest;
import org.apache.inlong.manager.pojo.sink.greenplum.GreenplumSinkRequest;
import org.apache.inlong.manager.pojo.sink.hbase.HBaseSinkRequest;
import org.apache.inlong.manager.pojo.sink.hdfs.HDFSSinkRequest;
import org.apache.inlong.manager.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.pojo.sink.iceberg.IcebergSinkRequest;
import org.apache.inlong.manager.pojo.sink.kafka.KafkaSinkRequest;
import org.apache.inlong.manager.pojo.sink.mysql.MySQLSinkRequest;
import org.apache.inlong.manager.pojo.sink.oracle.OracleSinkRequest;
import org.apache.inlong.manager.pojo.sink.postgresql.PostgreSQLSinkRequest;
import org.apache.inlong.manager.pojo.sink.sqlserver.SQLServerSinkRequest;
import org.apache.inlong.manager.pojo.sink.tdsqlpostgresql.TDSQLPostgreSQLSinkRequest;
import org.apache.inlong.manager.common.validation.UpdateValidation;
import org.hibernate.validator.constraints.Length;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.List;
import java.util.Map;

/**
 * Stream sink request
 */
@Data
@ApiModel("Stream sink request")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, visible = true, property = "sinkType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ClickHouseSinkRequest.class, name = SinkType.CLICKHOUSE),
        @JsonSubTypes.Type(value = ElasticsearchSinkRequest.class, name = SinkType.ELASTICSEARCH),
        @JsonSubTypes.Type(value = GreenplumSinkRequest.class, name = SinkType.GREENPLUM),
        @JsonSubTypes.Type(value = HBaseSinkRequest.class, name = SinkType.HBASE),
        @JsonSubTypes.Type(value = HDFSSinkRequest.class, name = SinkType.HDFS),
        @JsonSubTypes.Type(value = HiveSinkRequest.class, name = SinkType.HIVE),
        @JsonSubTypes.Type(value = IcebergSinkRequest.class, name = SinkType.ICEBERG),
        @JsonSubTypes.Type(value = DLCIcebergSinkRequest.class, name = SinkType.DLCICEBERG),
        @JsonSubTypes.Type(value = KafkaSinkRequest.class, name = SinkType.KAFKA),
        @JsonSubTypes.Type(value = MySQLSinkRequest.class, name = SinkType.MYSQL),
        @JsonSubTypes.Type(value = OracleSinkRequest.class, name = SinkType.ORACLE),
        @JsonSubTypes.Type(value = PostgreSQLSinkRequest.class, name = SinkType.POSTGRESQL),
        @JsonSubTypes.Type(value = SQLServerSinkRequest.class, name = SinkType.SQLSERVER),
        @JsonSubTypes.Type(value = TDSQLPostgreSQLSinkRequest.class, name = SinkType.TDSQLPOSTGRESQL),
})
public abstract class SinkRequest {

    @NotNull(groups = UpdateValidation.class)
    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @NotBlank(message = "inlongGroupId cannot be blank")
    @ApiModelProperty("Inlong group id")
    private String inlongGroupId;

    @NotBlank(message = "inlongStreamId cannot be blank")
    @ApiModelProperty("Inlong stream id")
    private String inlongStreamId;

    @NotBlank(message = "sinkType cannot be blank")
    @ApiModelProperty("Sink type, including: HIVE, ES, etc.")
    private String sinkType;

    @NotBlank(message = "sinkName cannot be blank")
    @Length(min = 1, max = 100, message = "sinkName length must be between 1 and 100")
    @Pattern(regexp = "^[a-z0-9_-]{1,100}$",
            message = "sinkName only supports lowercase letters, numbers, '-', or '_'")
    @ApiModelProperty("Sink name, unique in one stream")
    private String sinkName;

    @ApiModelProperty("Sink description")
    private String description;

    @ApiModelProperty("Inlong cluster name")
    private String inlongClusterName;

    @ApiModelProperty("Data node name")
    private String dataNodeName;

    @ApiModelProperty("Sort task name")
    private String sortTaskName;

    @ApiModelProperty("Sort consumer group")
    private String sortConsumerGroup;

    @ApiModelProperty(value = "Whether to enable create sink resource? 0: disable, 1: enable. Default is 1")
    private Integer enableCreateResource = 1;

    @ApiModelProperty("Sink field list")
    private List<SinkField> sinkFieldList;

    @ApiModelProperty("Other properties if needed")
    private Map<String, Object> properties = Maps.newHashMap();

    @ApiModelProperty(value = "Version number")
    private Integer version;

}
