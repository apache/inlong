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

package org.apache.inlong.manager.service.workflow.newconsumption;

import com.google.common.collect.Maps;
import io.swagger.annotations.ApiModelProperty;
import java.util.Map;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionInfo;
import org.apache.inlong.manager.service.workflow.BaseWorkflowFormType;
import org.apache.inlong.manager.workflow.exception.FormValidateException;
import org.apache.inlong.manager.common.util.Preconditions;

/**
 * New data consumption form
 */
@Data
public class NewConsumptionWorkflowForm extends BaseWorkflowFormType {

    public static final String FORM_NAME = "NewConsumptionWorkflowForm";

    @ApiModelProperty(value = "Data consumption information")
    private ConsumptionInfo consumptionInfo;

    @Override
    public void validate() throws FormValidateException {
        Preconditions.checkNotNull(consumptionInfo, "Data consumption information cannot be empty");
    }

    @Override
    public String getFormName() {
        return FORM_NAME;
    }

    @Override
    public String getBusinessId() {
        return consumptionInfo.getConsumerGroupId();
    }

    @Override
    public Map<String, Object> showInList() {
        Map<String, Object> show = Maps.newHashMap();
        if (consumptionInfo != null) {
            show.put("businessIdentifier", consumptionInfo.getBusinessIdentifier());
        }
        return show;
    }
}
