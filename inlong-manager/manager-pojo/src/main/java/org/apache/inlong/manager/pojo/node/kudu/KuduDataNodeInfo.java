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
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Kudu data node info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.KUDU)
@ApiModel("Kudu data node info")
public class KuduDataNodeInfo extends DataNodeInfo {

    @ApiModelProperty("Kudu masters, a comma separated list of 'host:port' pairs")
    private String masters;

    @ApiModelProperty("Default admin operation timeout in ms, default is 3000")
    private Integer defaultAdminOperationTimeoutMs;

    @ApiModelProperty("Default operation timeout in ms, default is 3000")
    private Integer defaultOperationTimeoutMs = 3000;

    @ApiModelProperty("Default socket read timeout in ms, default is 10000")
    private Integer defaultSocketReadTimeoutMs;

    @ApiModelProperty("Whether to enable the statistics collection function of the Kudu client, default is false.")
    private Boolean statisticsDisabled;

    public KuduDataNodeInfo() {
        setType(DataNodeType.KUDU);
    }

    @Override
    public DataNodeRequest genRequest() {
        return CommonBeanUtils.copyProperties(this, KuduDataNodeRequest::new);
    }
}
