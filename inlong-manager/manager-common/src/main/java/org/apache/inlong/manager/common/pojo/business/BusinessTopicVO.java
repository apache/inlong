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

package org.apache.inlong.manager.common.pojo.business;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamTopicVO;

/**
 * Topic View Object of the Business
 */
@Data
@ApiModel("Topic View Object of the Business")
public class BusinessTopicVO {

    @ApiModelProperty(value = "Business group id", required = true)
    private String inlongGroupId;

    @ApiModelProperty(value = "Middleware type, high throughput: TUBE, high consistency: PULSAR")
    private String middlewareType;

    @ApiModelProperty(value = "Tube topic name, or Pulsar namespace name")
    private String mqResourceObj;

    @ApiModelProperty(value = "Topic list, Tube corresponds to business group, there is only 1 topic, "
            + "Pulsar corresponds to data stream, there are multiple topics")
    private List<DataStreamTopicVO> dsTopicList;

    @ApiModelProperty(value = "Tube master URL")
    private String tubeMasterUrl;

    @ApiModelProperty(value = "Pulsar service URL")
    private String pulsarServiceUrl;

    @ApiModelProperty(value = "Pulsar admin URL")
    private String pulsarAdminUrl;

}
