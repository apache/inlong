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

package org.apache.inlong.manager.service.workflow.business.listener;

import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Initialize the listener for business information
 */
@Service
public class InitBusinessInfoListener implements ProcessEventListener {

    @Autowired
    private BusinessService businessService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.CREATE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        BusinessResourceProcessForm form = (BusinessResourceProcessForm) context.getProcessForm();
        BusinessInfo businessInfo = businessService.get(context.getProcessForm().getInlongGroupId());
        if (businessInfo != null) {
            form.setBusinessInfo(businessInfo);
        } else {
            throw new BusinessException(BizErrorCodeEnum.BUSINESS_NOT_FOUND);
        }
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
