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

package org.apache.inlong.manager.common.pojo.sink;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.collect.Maps;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.es.ElasticsearchSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.greenplum.GreenplumSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hbase.HBaseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hdfs.HdfsSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.oracle.OracleSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.sqlserver.SqlServerSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.tdsqlpostgresql.TDSQLPostgreSQLSinkRequest;
import org.apache.inlong.manager.common.pojo.common.UpdateValidation;
import org.hibernate.validator.constraints.Length;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.util.List;
import java.util.Map;

/**
 * Request of sink
 */
@Data
@ApiModel("Request of sink")
@JsonTypeInfo(use = Id.NAME, visible = true, property = "sinkType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ClickHouseSinkRequest.class, name = SinkType.SINK_CLICKHOUSE),
        @JsonSubTypes.Type(value = ElasticsearchSinkRequest.class, name = SinkType.SINK_ELASTICSEARCH),
        @JsonSubTypes.Type(value = GreenplumSinkRequest.class, name = SinkType.SINK_GREENPLUM),
        @JsonSubTypes.Type(value = HBaseSinkRequest.class, name = SinkType.SINK_HBASE),
        @JsonSubTypes.Type(value = HdfsSinkRequest.class, name = SinkType.SINK_HDFS),
        @JsonSubTypes.Type(value = HiveSinkRequest.class, name = SinkType.SINK_HIVE),
        @JsonSubTypes.Type(value = IcebergSinkRequest.class, name = SinkType.SINK_ICEBERG),
        @JsonSubTypes.Type(value = KafkaSinkRequest.class, name = SinkType.SINK_KAFKA),
        @JsonSubTypes.Type(value = MySQLSinkRequest.class, name = SinkType.SINK_MYSQL),
        @JsonSubTypes.Type(value = OracleSinkRequest.class, name = SinkType.SINK_ORACLE),
        @JsonSubTypes.Type(value = PostgresSinkRequest.class, name = SinkType.SINK_POSTGRES),
        @JsonSubTypes.Type(value = SqlServerSinkRequest.class, name = SinkType.SINK_SQLSERVER),
        @JsonSubTypes.Type(value = TDSQLPostgreSQLSinkRequest.class, name = SinkType.SINK_TDSQLPOSTGRESQL),
})
public class SinkRequest {

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

}
