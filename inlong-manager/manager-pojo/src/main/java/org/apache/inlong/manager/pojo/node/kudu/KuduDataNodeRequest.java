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

package org.apache.inlong.manager.pojo.node.kudu;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Kudu data node request
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.KUDU)
@ApiModel("Kudu data node request")
public class KuduDataNodeRequest extends DataNodeRequest {

    @ApiModelProperty("Kudu masters, a comma separated list of 'host:port' pairs")
    private String masters;

    @ApiModelProperty("Default admin operation timeout in ms, default is 30000")
    private Integer defaultAdminOperationTimeoutMs = 30000;

    @ApiModelProperty("Default operation timeout in ms, default is 30000")
    private Integer defaultOperationTimeoutMs = 30000;

    @ApiModelProperty("Default socket read timeout in ms, default is 10000")
    private Integer defaultSocketReadTimeoutMs = 10000;

    @ApiModelProperty("Whether to enable the statistics collection function of the Kudu client, default is false.")
    private Boolean statisticsDisabled = false;

    public KuduDataNodeRequest() {
        setType(DataNodeType.KUDU);
    }
}
