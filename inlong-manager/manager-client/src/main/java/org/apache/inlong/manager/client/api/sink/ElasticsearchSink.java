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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.stream.SinkField;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;

import java.util.List;
import java.util.Map;

/**
 * Elasticsearch sink.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Elasticsearch sink configuration")
public class ElasticsearchSink extends StreamSink {

    @ApiModelProperty(value = "Sink type", required = true)
    private SinkType sinkType = SinkType.ELASTICSEARCH;

    @ApiModelProperty("Elasticsearch Host")
    private String host;

    @ApiModelProperty("Elasticsearch Port")
    private Integer port;

    @ApiModelProperty("Authentication for elasticsearch")
    private DefaultAuthentication authentication;

    @ApiModelProperty("Elasticsearch index name")
    private String indexName;

    @ApiModelProperty("Flush interval, unit: second, default is 1s")
    private Integer flushInterval;

    @ApiModelProperty("Flush when record number reaches flushRecord")
    private Integer flushRecord;

    @ApiModelProperty("Write max retry times, default is 3")
    private Integer retryTimes;

    @ApiModelProperty("Create topic or not")
    private boolean needCreated;

    @ApiModelProperty("Properties for elasticsearch")
    private Map<String, Object> properties;

    @ApiModelProperty("Field definitions for elasticsearch")
    private List<SinkField> sinkFields;

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.NONE;
    }
}
