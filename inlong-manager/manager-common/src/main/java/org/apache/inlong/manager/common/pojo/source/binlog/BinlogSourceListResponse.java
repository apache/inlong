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

package org.apache.inlong.manager.common.pojo.source.binlog;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;

/**
 * Response of binlog source list
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ApiModel("Response of binlog source paging list")
public class BinlogSourceListResponse extends SourceListResponse {

    @ApiModelProperty("Source database name")
    private String dbName;

    @ApiModelProperty("Source table name")
    private String tableName;

    @ApiModelProperty(value = "Middleware type, such as: TUBE, PULSAR")
    private String middlewareType;

    @ApiModelProperty(value = "Topic of Tube")
    private String tubeTopic;

    @ApiModelProperty(value = "Cluster address of Tube")
    private String tubeCluster;

    @ApiModelProperty(value = "Namespace of Pulsar")
    private String pulsarNamespace;

    @ApiModelProperty(value = "Topic of Pulsar")
    private String pulsarTopic;

    @ApiModelProperty(value = "Cluster address of Pulsar")
    private String pulsarCluster;

}
