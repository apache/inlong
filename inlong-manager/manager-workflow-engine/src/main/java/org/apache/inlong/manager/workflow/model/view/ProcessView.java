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

package org.apache.inlong.manager.workflow.model.view;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.workflow.model.ProcessState;
import org.apache.inlong.manager.workflow.model.instance.ProcessInstance;

/**
 * Application form information
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Application form information")
public class ProcessView {

    /**
     * Application form ID
     */
    @ApiModelProperty(value = "application form ID")
    private Integer id;

    /**
     * Process name-English key
     */
    @ApiModelProperty(value = "process name-English key")
    private String name;

    /**
     * Process display name-Chinese
     */
    @ApiModelProperty(value = "process display name-Chinese")
    private String displayName;

    /**
     * Process classification
     */
    @ApiModelProperty(value = "process classification")
    private String type;

    /**
     * Process title
     */
    @ApiModelProperty(value = "process title")
    private String title;

    /**
     * Applicant
     */
    @ApiModelProperty(value = "applicant")
    private String applicant;

    /**
     * Process status
     */
    @ApiModelProperty(value = "process status")
    private ProcessState state;

    /**
     * Application time
     */
    @ApiModelProperty(value = "application time")
    private Date startTime;

    /**
     * End Time
     */
    @ApiModelProperty(value = "end Time")
    private Date endTime;

    /**
     * Form information
     */
    @ApiModelProperty(value = "form information-JSON")
    private Object formData;

    /**
     * Extended Information
     */
    @ApiModelProperty(value = "extended Information-JSON")
    private Object ext;

    public static ProcessView fromProcessInstance(ProcessInstance processInstance) {
        return ProcessView.builder()
                .id(processInstance.getId())
                .name(processInstance.getName())
                .displayName(processInstance.getDisplayName())
                .type(processInstance.getType())
                .title(processInstance.getTitle())
                .applicant(processInstance.getApplicant())
                .state(ProcessState.valueOf(processInstance.getState()))
                .startTime(processInstance.getStartTime())
                .endTime(processInstance.getEndTime())
                .formData(JsonUtils.parse(processInstance.getFormData()))
                .ext(JsonUtils.parse(processInstance.getExt()))
                .build();

    }

}
