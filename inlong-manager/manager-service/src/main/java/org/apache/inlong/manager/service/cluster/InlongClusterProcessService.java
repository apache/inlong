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

package org.apache.inlong.manager.service.cluster;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.ProcessName;
import org.apache.inlong.manager.common.enums.ProcessStatus;
import org.apache.inlong.manager.common.threadPool.VisiableThreadPoolTaskExecutor;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.group.GroupFullInfo;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserInfo;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.pojo.workflow.form.process.ClusterResourceProcessForm;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.workflow.WorkflowService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.manager.common.consts.InlongConstants.ALIVE_TIME_MS;
import static org.apache.inlong.manager.common.consts.InlongConstants.CORE_POOL_SIZE;
import static org.apache.inlong.manager.common.consts.InlongConstants.MAX_POOL_SIZE;
import static org.apache.inlong.manager.common.consts.InlongConstants.QUEUE_SIZE;

/**
 * Operation related to inlong cluster process
 */
@Slf4j
@Service
public class InlongClusterProcessService {

    private static final ExecutorService EXECUTOR_SERVICE = new VisiableThreadPoolTaskExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            ALIVE_TIME_MS,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(QUEUE_SIZE),
            new ThreadFactoryBuilder().setNameFormat("inlong-cluster-process-%s").build(),
            new CallerRunsPolicy());

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private WorkflowService workflowService;

    /**
     * Create cluster in synchronous/asynchronous way.
     */
    public boolean startProcess(String clusterTag, String operator, boolean sync) {
        log.info("start cluster process for clusterTag={}, operator={}", clusterTag, operator);

        List<GroupFullInfo> groupFullInfoList = new ArrayList<>();
        LoginUserUtils.getLoginUser().getRoles().add(UserRoleCode.INLONG_SERVICE);
        groupFullInfoList.addAll(groupService.getGroupByClusterTag(clusterTag));
        groupFullInfoList.addAll(groupService.getGroupByBackUpClusterTag(clusterTag));
        Preconditions.expectTrue(CollectionUtils.isNotEmpty(groupFullInfoList),
                ErrorCodeEnum.GROUP_NOT_FOUND.getMessage());
        ClusterResourceProcessForm clusterResourceProcessForm = ClusterResourceProcessForm.getProcessForm(clusterTag,
                groupFullInfoList);
        LoginUserUtils.getLoginUser().getRoles().remove(UserRoleCode.INLONG_SERVICE);
        ProcessName processName = ProcessName.CREATE_CLUSTER_RESOURCE;
        if (sync) {
            WorkflowResult workflowResult = workflowService.start(processName, operator, clusterResourceProcessForm);
            ProcessStatus processStatus = workflowResult.getProcessInfo().getStatus();
            return processStatus == ProcessStatus.COMPLETED;
        } else {
            log.info("start cluster process for clusterTag={}, form={}", clusterTag, clusterResourceProcessForm);
            UserInfo userInfo = LoginUserUtils.getLoginUser();
            EXECUTOR_SERVICE.execute(
                    () -> workflowService.startAsync(processName, userInfo, clusterResourceProcessForm));
            return true;
        }
    }

}
