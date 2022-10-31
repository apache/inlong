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

package org.apache.inlong.manager.service.listener.queue;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.ProcessName;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.common.enums.TaskStatus;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.TaskResponse;
import org.apache.inlong.manager.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperatorFactory;
import org.apache.inlong.manager.service.workflow.WorkflowService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * Create message queue resources,
 * such as Pulsar Topic and Subscription, TubeMQ Topic and ConsumerGroup, etc.
 */
@Slf4j
@Service
public class QueueResourceListener implements QueueOperateListener {

    private final ExecutorService executorService = new ThreadPoolExecutor(
            20,
            40,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("inlong-stream-process-%s").build(),
            new CallerRunsPolicy());

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private QueueResourceOperatorFactory queueOperatorFactory;
    @Autowired
    private WorkflowService workflowService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        if (!isGroupProcessForm(context)) {
            return false;
        }
        GroupResourceProcessForm processForm = (GroupResourceProcessForm) context.getProcessForm();
        return InlongConstants.STANDARD_MODE.equals(processForm.getGroupInfo().getLightweight());
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        GroupResourceProcessForm groupProcessForm = (GroupResourceProcessForm) context.getProcessForm();
        final String groupId = groupProcessForm.getInlongGroupId();
        // ensure the inlong group exists
        InlongGroupInfo groupInfo = groupService.get(groupId);
        if (groupInfo == null) {
            String msg = "inlong group not found with groupId=" + groupId;
            log.error(msg);
            throw new WorkflowListenerException(msg);
        }

        if (InlongConstants.DISABLE_CREATE_RESOURCE.equals(groupInfo.getEnableCreateResource())) {
            log.warn("skip to execute QueueResourceListener as disable create resource for groupId={}", groupId);
            return ListenerResult.success("skip - disable create resource");
        }

        QueueResourceOperator queueOperator = queueOperatorFactory.getInstance(groupInfo.getMqType());
        GroupOperateType operateType = groupProcessForm.getGroupOperateType();
        String operator = context.getOperator();
        switch (operateType) {
            case INIT:
                // queueOperator.createQueueForGroup(groupInfo, operator);
                List<InlongStreamInfo> streamList = groupProcessForm.getStreamInfos();
                List<CompletableFuture<WorkflowResult>> futures = new ArrayList<>();
                for (InlongStreamInfo streamInfo : streamList) {
                    StreamResourceProcessForm processForm = genStreamProcessForm(groupInfo, streamInfo, operateType);
                    CompletableFuture<WorkflowResult> future = CompletableFuture.supplyAsync(
                            () -> workflowService.start(ProcessName.CREATE_STREAM_RESOURCE, operator, processForm),
                            executorService);
                    // // futures.add(future);
                    // future.thenAccept((result) -> {
                    //     List<TaskResponse> taskResponse = result.getNewTasks();
                    //     int len = taskResponse.size();
                    //     if (taskResponse.get(len - 1).getStatus().equals(TaskStatus.FAILED)) {
                    //         String errMsg = String.format(
                    //                 "failed to execute stream workflow for groupId=%s, streamId=%s",
                    //                 groupId, streamInfo.getInlongStreamId());
                    //         log.error(errMsg);
                    //         throw new WorkflowListenerException(errMsg);
                    //     } else {
                    //         log.info("success to execute stream workflow for groupId={}, streamId={}", groupId,
                    //                 streamInfo.getInlongStreamId());
                    //     }
                    // });
                    future.exceptionally((e) -> {
                        String errMsg = String.format("failed to execute stream workflow for groupId=%s, streamId=%s",
                                groupId, streamInfo.getInlongStreamId());
                        log.error(errMsg);
                        throw new WorkflowListenerException(errMsg, e);
                    });
                    String streamId = streamInfo.getInlongStreamId();
                    try {
                        WorkflowResult result = future.get();
                        List<TaskResponse> taskResponse = result.getNewTasks();
                        int len = taskResponse.size();
                        if (taskResponse.get(len - 1).getStatus().equals(TaskStatus.FAILED)) {
                            String errMsg = String.format(
                                    "failed to execute stream workflow for groupId=%s, streamId=%s",
                                    groupId, streamId);
                            log.error(errMsg);
                            throw new WorkflowListenerException(errMsg);
                        }
                    } catch (Exception e) {
                        String errMsg = String.format("failed to execute stream workflow for groupId=%s, streamId=%s",
                                groupId, streamId);
                        log.error(errMsg);
                        throw new WorkflowListenerException(errMsg, e);
                    }
                }
                log.info("success to execute stream workflow for groupId={}", groupId);
                break;
            case DELETE:
                queueOperator.deleteQueueForGroup(groupInfo, operator);
                break;
            default:
                log.warn("unsupported operate={} for inlong group", operateType);
                break;
        }

        log.info("success to execute QueueResourceListener for groupId={}, operateType={}", groupId, operateType);
        return ListenerResult.success("success");
    }

    private StreamResourceProcessForm genStreamProcessForm(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo,
            GroupOperateType operateType) {
        StreamResourceProcessForm processForm = new StreamResourceProcessForm();
        processForm.setGroupInfo(groupInfo);
        processForm.setStreamInfo(streamInfo);
        processForm.setGroupOperateType(operateType);
        return processForm;
    }

}
