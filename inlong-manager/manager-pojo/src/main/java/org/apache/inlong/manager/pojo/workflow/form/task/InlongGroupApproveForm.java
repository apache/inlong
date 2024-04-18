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

package org.apache.inlong.manager.pojo.workflow.form.task;

import org.apache.inlong.manager.common.exceptions.FormValidateException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.group.InlongGroupApproveRequest;
import org.apache.inlong.manager.pojo.stream.InlongStreamApproveRequest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * The approval form of the inlong group
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class InlongGroupApproveForm extends BaseTaskForm {

    public static final String FORM_NAME = "InlongGroupApproveForm";

    @ApiModelProperty(value = "Inlong group approve info")
    private InlongGroupApproveRequest groupApproveInfo;

    @ApiModelProperty(value = "All inlong stream info under the inlong group, including the sink info")
    private List<InlongStreamApproveRequest> streamApproveInfoList;

    @ApiModelProperty(value = "Inlong group approve full info list")
    private List<GroupApproveFullRequest> groupApproveFullInfoList;

    @Override
    public void validate() throws FormValidateException {
        Preconditions.expectTrue(groupApproveInfo != null || CollectionUtils.isNotEmpty(groupApproveFullInfoList),
                "inlong group approve info is empty");
    }

    @Override
    public String getFormName() {
        return FORM_NAME;
    }

    @JsonIgnore
    public List<GroupApproveFullRequest> getApproveFullRequest() {
        List<GroupApproveFullRequest> result = new ArrayList<>();
        if (groupApproveInfo != null) {
            result.add(new GroupApproveFullRequest(groupApproveInfo, streamApproveInfoList));
        }
        if (CollectionUtils.isNotEmpty(groupApproveFullInfoList)) {
            result.addAll(groupApproveFullInfoList);
        }
        return result;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GroupApproveFullRequest {

        private InlongGroupApproveRequest groupApproveInfo;

        private List<InlongStreamApproveRequest> streamApproveInfoList;

    }

}
