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

package org.apache.inlong.manager.common.workflow.bussiness;

import com.google.common.collect.Maps;
import io.swagger.annotations.ApiModelProperty;
import java.util.Locale;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.inlong.manager.common.exceptions.FormValidateException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.common.workflow.BaseWorkflowFormType;

@Data
public class UpdateBusinessWorkflowForm extends BaseWorkflowFormType {

    public static final String FORM_NAME = "UpdateBusinessWorkflowForm";

    /**
     * Used to control the operation to update businessWorkflow
     */
    public enum OperateType {
        STARTUP, SUSPEND, RESTART, DELETE, START_TRANS
    }

    @ApiModelProperty(value = "Access business information", required = true)
    private BusinessInfo businessInfo;

    @Getter
    @Setter
    @ApiModelProperty(value = "OperateType to define the update operation", required = true)
    private OperateType operateType;

    @Override
    public void validate() throws FormValidateException {
        Preconditions.checkNotNull(businessInfo, "business info is empty");
        Preconditions.checkNotNull(operateType, "operate type is empty");
    }

    @Override
    public String getFormName() {
        return FORM_NAME;
    }

    @Override
    public String getInlongGroupId() {
        return businessInfo.getInlongGroupId();
    }

    @Override
    public Map<String, Object> showInList() {
        Map<String, Object> show = Maps.newHashMap();
        show.put("groupId", businessInfo.getInlongGroupId());
        show.put("operateType", operateType.name().toLowerCase(Locale.ROOT));
        return show;
    }
}
