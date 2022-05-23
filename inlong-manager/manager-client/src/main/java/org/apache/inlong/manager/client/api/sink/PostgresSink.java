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
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.stream.SinkField;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;

/**
 * Postgres sink.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Postgres sink configuration")
public class PostgresSink extends StreamSink {

    @ApiModelProperty(value = "Sink type", required = true)
    private SinkType sinkType = SinkType.POSTGRES;

    @ApiModelProperty("Postgres JDBC URL")
    private String jdbcUrl;

    @ApiModelProperty("Target database name")
    private String dbName;

    @ApiModelProperty("Authentication for postgres")
    private DefaultAuthentication authentication;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Field definitions for postgres")
    private List<SinkField> sinkFields;

    @ApiModelProperty("Primary key")
    private String primaryKey;

    /**
     * get data format
     * @return just NONE
     */
    public DataFormat getDataFormat() {
        return DataFormat.NONE;
    }
}
