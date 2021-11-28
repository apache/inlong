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

package org.apache.inlong.manager.service.workflow.newconsumption.listener;

import java.util.Collections;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.ConsumptionStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionInfo;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ConsumptionEntity;
import org.apache.inlong.manager.dao.mapper.ClusterInfoMapper;
import org.apache.inlong.manager.dao.mapper.ConsumptionEntityMapper;
import org.apache.inlong.manager.service.thirdpart.mq.TubeMqOptService;
import org.apache.inlong.manager.service.workflow.newconsumption.NewConsumptionApproveForm;
import org.apache.inlong.manager.service.workflow.newconsumption.NewConsumptionWorkflowDefinition;
import org.apache.inlong.manager.service.workflow.newconsumption.NewConsumptionWorkflowForm;
import org.apache.inlong.manager.workflow.core.QueryService;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;
import org.apache.inlong.manager.workflow.model.view.TaskQuery;
import org.apache.inlong.manager.workflow.util.WorkflowFormParserUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Added data consumption process complete archive event listener
 */
@Slf4j
@Component
public class ConsumptionCompleteProcessListener implements ProcessEventListener {

    @Autowired
    private QueryService queryService;

    @Autowired
    private ConsumptionEntityMapper consumptionEntityMapper;

    @Autowired
    private ClusterInfoMapper clusterInfoMapper;

    @Autowired
    private TubeMqOptService tubeMqOptService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {

        NewConsumptionWorkflowForm workflowForm = (NewConsumptionWorkflowForm) context.getProcessForm();
        NewConsumptionApproveForm adminApproveForm = getAdminApproveForm(context);

        workflowForm.getConsumptionInfo().setConsumerGroupId(adminApproveForm.getConsumerGroupId());
        updateConsumerInfo(workflowForm.getConsumptionInfo().getId(), adminApproveForm.getConsumerGroupId());

        String middlewareType = workflowForm.getConsumptionInfo().getMiddlewareType();

        if (BizConstant.MIDDLEWARE_TUBE.equalsIgnoreCase(middlewareType)) {
            createTubeConsumerGroup(workflowForm.getConsumptionInfo());
            return ListenerResult.success("Create Tube Consumer Group");
        }

        throw new BusinessException(BizErrorCodeEnum.INVALID_PARAMETER,
                "Middleware type [" + middlewareType + "] not support");

    }

    private NewConsumptionApproveForm getAdminApproveForm(WorkflowContext context) {
        TaskInstance adminTask = queryService.listTask(TaskQuery.builder()
                .processInstId(context.getProcessInstance().getId())
                .name(NewConsumptionWorkflowDefinition.UT_ADMINT_NAME)
                .build())
                .stream()
                .findFirst()
                .orElseThrow(() -> new BusinessException(BizErrorCodeEnum.WORKFLOW_EXE_FAILED,
                        "workflow err,not found task " + NewConsumptionWorkflowDefinition.UT_ADMINT_NAME));

        Process process = context.getProcess();
        NewConsumptionApproveForm form = WorkflowFormParserUtils.parseTaskForm(adminTask, process);
        Preconditions.checkNotNull(form, "form can't be null");
        return form;
    }

    private void updateConsumerInfo(Integer consumerId, String consumerGroupId) {
        ConsumptionEntity update = new ConsumptionEntity();
        update.setId(consumerId);
        update.setStatus(ConsumptionStatus.APPROVED.getStatus());
        update.setConsumerGroupId(consumerGroupId);
        update.setModifyTime(new Date());
        consumptionEntityMapper.updateByPrimaryKeySelective(update);
    }

    private void createTubeConsumerGroup(ConsumptionInfo consumptionInfo) {
        AddTubeConsumeGroupRequest addTubeConsumeGroupRequest = new AddTubeConsumeGroupRequest();
        addTubeConsumeGroupRequest.setClusterId(1); // TODO is cluster id needed?
        addTubeConsumeGroupRequest.setCreateUser(consumptionInfo.getCreator());
        AddTubeConsumeGroupRequest.GroupNameJsonSetBean bean = new AddTubeConsumeGroupRequest.GroupNameJsonSetBean();
        bean.setTopicName(consumptionInfo.getTopic());
        bean.setGroupName(consumptionInfo.getConsumerGroupId());
        addTubeConsumeGroupRequest.setGroupNameJsonSet(Collections.singletonList(bean));

        try {
            tubeMqOptService.createNewConsumerGroup(addTubeConsumeGroupRequest);
        } catch (BusinessException e) {
            throw e;
        } catch (Exception e) {
            throw new BusinessException(BizErrorCodeEnum.CONSUMER_GROUP_CREATE_FAILED);
        }
    }

    @Override
    public boolean async() {
        return false;
    }

}
