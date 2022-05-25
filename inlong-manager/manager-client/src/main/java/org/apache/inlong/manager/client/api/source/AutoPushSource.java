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

package org.apache.inlong.manager.client.api.source;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.DataFormat;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;
import org.apache.inlong.manager.common.enums.SourceType;

/**
 * Agent push source.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@ApiModel("Base configuration for AutoPush collection")
public class AutoPushSource extends StreamSource {

    @ApiModelProperty(value = "DataSource type", required = true)
    private SourceType sourceType = SourceType.AUTO_PUSH;

    @ApiModelProperty("SyncType")
    private SyncType syncType = SyncType.INCREMENT;

    @ApiModelProperty("Data format type")
    private DataFormat dataFormat = DataFormat.JSON;

    @ApiModelProperty(value = "DataProxy group name, used when the user enables local configuration")
    private String dataProxyGroup;
}
