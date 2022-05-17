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
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.stream.SinkField;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;

import java.util.List;

/**
 * Hbase sink.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Hive sink configuration")
public class HbaseSink extends StreamSink {

    @ApiModelProperty(value = "Sink type", required = true)
    private SinkType sinkType = SinkType.HBASE;

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Namespace")
    private String nameSpace;

    @ApiModelProperty("Row key")
    private String rowKey;

    @ApiModelProperty("Zookeeper quorm")
    private String zookeeperQuorum;

    @ApiModelProperty("Sink buffer flush maxsize")
    private String sinkBufferFlushMaxSize;

    @ApiModelProperty("Zookeeper znode parent")
    private String zookeeperZnodeParent;

    @ApiModelProperty("Sink buffer flush max rows")
    private String sinkBufferFlushMaxRows;

    @ApiModelProperty("Sink buffer flush interval")
    private String sinkBufferFlushInterval;

    @ApiModelProperty("Create table or not")
    private boolean needCreated;

    @ApiModelProperty("Field definitions for hbase")
    private List<SinkField> sinkFields;

    @ApiModelProperty("Data format type for stream sink")
    private DataFormat dataFormat;
}
