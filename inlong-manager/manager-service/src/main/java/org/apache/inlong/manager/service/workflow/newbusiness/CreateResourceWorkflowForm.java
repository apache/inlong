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

import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.service.workflow.BaseWorkflowFormType;
import org.apache.inlong.manager.workflow.exception.FormValidateException;

import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Form of create business resource
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class CreateResourceWorkflowForm extends BaseWorkflowFormType {

    public static final String FORM_NAME = "CreateResourceWorkflowForm";

    private BusinessInfo businessInfo;

    private String dataStreamIdentifier;

    public BusinessInfo getBusinessInfo() {
        return businessInfo;
    }

    public void setBusinessInfo(BusinessInfo businessInfo) {
        this.businessInfo = businessInfo;
    }

    @Override
    public void validate() throws FormValidateException {
    }

    @Override
    public String getFormName() {
        return FORM_NAME;
    }

    @Override
    public String getBusinessId() {
        return businessInfo.getBusinessIdentifier();
    }

    public String getDataStreamIdentifier() {
        return dataStreamIdentifier;
    }

    public void setDataStreamIdentifier(String dataStreamIdentifier) {
        this.dataStreamIdentifier = dataStreamIdentifier;
    }

    @Override
    public Map<String, Object> showInList() {
        Map<String, Object> show = Maps.newHashMap();
        show.put("businessIdentifier", businessInfo.getBusinessIdentifier());
        return show;
    }
}
