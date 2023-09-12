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

package org.apache.inlong.manager.pojo.node.cls;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Tencent cloud log service data node info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.ELASTICSEARCH)
@ApiModel("Tencent cloud log service data node info")
public class TencentClsDataNodeInfo extends DataNodeInfo {

    /**
     *Tencent cloud log service master account
     */
    private String mainAccountID;

    /**
     *Tencent cloud log service subAccount
     */
    private String subAccountID;

    /**
     * Tencent cloud log service send api secretKey
     */
    private String sendSecretKey;

    /**
     * Tencent cloud log service send api secretId
     */
    private String sendSecretId;

    /**
     * Tencent cloud log service manage api secretKey
     */
    private String manageSecretKey;

    /**
     * Tencent cloud log service manage api secretId
     */
    private String manageSecretId;

    /**
     * Tencent cloud log service endpoint
     */
    private String endpoint;

    /**
     * Tencent cloud log service region
     */
    private String region;

    /**
     * Tencent cloud log service logSet id
     */
    private String logSetID;

    public TencentClsDataNodeInfo() {
        setType(DataNodeType.CLS);
    }

    @Override
    public DataNodeRequest genRequest() {
        return CommonBeanUtils.copyProperties(this, TencentClsDataNodeRequest::new);
    }
}
