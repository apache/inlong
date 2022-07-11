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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.es.ElasticsearchSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.greenplum.GreenplumSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.hbase.HBaseSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.hdfs.HdfsSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.iceberg.IcebergSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.mysql.MySQLSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.oracle.OracleSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.postgres.PostgresSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.sqlserver.SqlServerSinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.tdsqlpostgresql.TDSQLPostgreSQLSinkListResponse;

import java.util.Date;
import java.util.Map;

/**
 * Response of the sink list
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@JsonTypeInfo(use = Id.NAME, visible = true, property = "sinkType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ClickHouseSinkListResponse.class, name = SinkType.SINK_CLICKHOUSE),
        @JsonSubTypes.Type(value = ElasticsearchSinkListResponse.class, name = SinkType.SINK_ELASTICSEARCH),
        @JsonSubTypes.Type(value = GreenplumSinkListResponse.class, name = SinkType.SINK_GREENPLUM),
        @JsonSubTypes.Type(value = HBaseSinkListResponse.class, name = SinkType.SINK_HBASE),
        @JsonSubTypes.Type(value = HdfsSinkListResponse.class, name = SinkType.SINK_HDFS),
        @JsonSubTypes.Type(value = HiveSinkListResponse.class, name = SinkType.SINK_HIVE),
        @JsonSubTypes.Type(value = IcebergSinkListResponse.class, name = SinkType.SINK_ICEBERG),
        @JsonSubTypes.Type(value = KafkaSinkListResponse.class, name = SinkType.SINK_KAFKA),
        @JsonSubTypes.Type(value = MySQLSinkListResponse.class, name = SinkType.SINK_MYSQL),
        @JsonSubTypes.Type(value = OracleSinkListResponse.class, name = SinkType.SINK_ORACLE),
        @JsonSubTypes.Type(value = PostgresSinkListResponse.class, name = SinkType.SINK_POSTGRES),
        @JsonSubTypes.Type(value = SqlServerSinkListResponse.class, name = SinkType.SINK_SQLSERVER),
        @JsonSubTypes.Type(value = TDSQLPostgreSQLSinkListResponse.class, name = SinkType.SINK_TDSQLPOSTGRESQL),
})
public class SinkListResponse {

    @ApiModelProperty(value = "Primary key")
    private Integer id;

    @ApiModelProperty(value = "Status")
    private Integer status;

    @ApiModelProperty(value = "Inlong group id")
    private String inlongGroupId;

    @ApiModelProperty(value = "Inlong stream id")
    private String inlongStreamId;

    @ApiModelProperty("Sink type, including: HIVE, ES, etc.")
    private String sinkType;

    @ApiModelProperty("Sink name, unique in one stream.")
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

    @ApiModelProperty(value = "Whether to enable create sink resource? 0: disable, 1: enable. default is 1")
    private Integer enableCreateResource;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date createTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date modifyTime;

    @ApiModelProperty("Properties for sink")
    private Map<String, Object> properties;
}
