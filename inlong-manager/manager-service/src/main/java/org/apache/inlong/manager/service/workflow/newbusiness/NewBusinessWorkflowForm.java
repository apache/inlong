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

package org.apache.inlong.manager.service.workflow.newbusiness;

import com.google.common.collect.Maps;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamSummaryInfo;
import org.apache.inlong.manager.service.workflow.BaseWorkflowFormType;
import org.apache.inlong.manager.workflow.exception.FormValidateException;
import org.apache.inlong.manager.common.util.Preconditions;

/**
 * New business workflow form information
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class NewBusinessWorkflowForm extends BaseWorkflowFormType {

    public static final String FORM_NAME = "NewBusinessWorkflowForm";

    @ApiModelProperty(value = "Access business information", required = true)
    private BusinessInfo businessInfo;

    @ApiModelProperty(value = "All data stream information under the business, including the storage information")
    private List<DataStreamSummaryInfo> streamInfoList;

    @Override
    public void validate() throws FormValidateException {
        Preconditions.checkNotNull(businessInfo, "business info is empty");
    }

    @Override
    public String getFormName() {
        return FORM_NAME;
    }

    @Override
    public String getBusinessId() {
        return businessInfo.getBusinessIdentifier();
    }

    @Override
    public Map<String, Object> showInList() {
        Map<String, Object> show = Maps.newHashMap();
        show.put("businessIdentifier", businessInfo.getBusinessIdentifier());
        return show;
    }
}
