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

package org.apache.inlong.manager.common.pojo.sink.hbase;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;

/**
 * Response of Hbase sink list
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ApiModel("Response of Hbase sink paging list")
public class HbaseSinkListResponse extends SinkListResponse {

    @ApiModelProperty("Target table name")
    private String tableName;

    @ApiModelProperty("Namespace")
    private String namespace;

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

}
