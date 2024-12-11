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

package org.apache.inlong.manager.schedule.dolphinscheduler;

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.schedule.ScheduleEngine;
import org.apache.inlong.manager.schedule.exception.DolphinScheduleException;

import com.google.common.annotations.VisibleForTesting;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PROCESS_DESC;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PROCESS_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PROJECT_DESC;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PROJECT_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_OFFLINE_STATE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_ONLINE_STATE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROCESS_QUERY_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROCESS_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROJECT_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_SCHEDULE_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TASK_CODE_URL;

/**
 * The default implementation of DolphinScheduler engine based on DolphinScheduler API. Response for processing
 * the register/unregister/update requests from {@link DolphinScheduleClient}
 */
@Data
@Service
public class DolphinScheduleEngine implements ScheduleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DolphinScheduleEngine.class);

    @Value("${schedule.engine.inlong.manager.url:http://127.0.0.1:8083}")
    private String inlongManagerUrl;

    @Value("${default.admin.user:admin}")
    private String username;

    @Value("${default.admin.password:inlong}")
    private String password;

    @Value("${schedule.engine.dolphinscheduler.url:http://127.0.0.1:12345/dolphinscheduler}")
    private String dolphinUrl;

    @Value("${schedule.engine.dolphinscheduler.token:default_token_value}")
    private String token;

    @Resource
    private DolphinScheduleOperator dolphinScheduleOperator;

    private long projectCode;
    private String projectBaseUrl;
    private final Map<Long, String> scheduledProcessMap;

    @PostConstruct
    public void init() {
        this.projectBaseUrl = dolphinUrl + DS_PROJECT_URL;
    }

    public DolphinScheduleEngine(String inlongManagerUrl, String username, String password,
            String dolphinUrl,
            String token) {
        this.inlongManagerUrl = inlongManagerUrl;
        this.username = username;
        this.password = password;
        this.dolphinUrl = dolphinUrl;
        this.token = token;
        this.scheduledProcessMap = new ConcurrentHashMap<>();
    }

    public DolphinScheduleEngine() {
        this.scheduledProcessMap = new ConcurrentHashMap<>();
    }

    /**
     * check if there already exists a project for inlong offline schedule
     * if no then build a new project for inlong-group-id in DolphinScheduler
     */
    @Override
    public void start() {
        LOGGER.info("Starting dolphin scheduler engine, Checking project exists...");
        long code = dolphinScheduleOperator.checkAndGetUniqueId(projectBaseUrl, token, DS_DEFAULT_PROJECT_NAME);
        if (code != 0) {
            LOGGER.info("Project exists, project code: {}", code);
            this.projectCode = code;

            LOGGER.info("Starting synchronize existing process definition");
            String queryProcessDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL + DS_PROCESS_QUERY_URL;
            scheduledProcessMap.putAll(dolphinScheduleOperator.queryAllProcessDef(queryProcessDefUrl, token));

        } else {
            LOGGER.info("There is no inlong offline project exists, default project will be created");
            this.projectCode =
                    dolphinScheduleOperator.creatProject(projectBaseUrl, token, DS_DEFAULT_PROJECT_NAME,
                            DS_DEFAULT_PROJECT_DESC);
        }
    }

    /**
     * Handle schedule register.
     * @param scheduleInfo schedule info to register
     */
    @Override
    @VisibleForTesting
    public boolean handleRegister(ScheduleInfo scheduleInfo) {
        start();
        String processDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL;
        String scheduleUrl = projectBaseUrl + "/" + projectCode + DS_SCHEDULE_URL;
        String processName = scheduleInfo.getInlongGroupId() + DS_DEFAULT_PROCESS_NAME;
        String processDesc = DS_DEFAULT_PROCESS_DESC + scheduleInfo.getInlongGroupId();

        LOGGER.info("Dolphin Scheduler handle register begin for {}, Checking process definition id uniqueness...",
                scheduleInfo.getInlongGroupId());
        try {
            long processDefCode = dolphinScheduleOperator.checkAndGetUniqueId(processDefUrl, token, processName);

            boolean online = false;
            if (processDefCode != 0 || scheduledProcessMap.containsKey(processDefCode)) {

                // process definition already exists, delete and rebuild
                LOGGER.info("Process definition exists, process definition id: {}, deleting...", processDefCode);
                if (dolphinScheduleOperator.releaseProcessDef(processDefUrl, processDefCode, token, DS_OFFLINE_STATE)) {
                    dolphinScheduleOperator.deleteProcessDef(processDefUrl, token, processDefCode);
                    scheduledProcessMap.remove(processDefCode);
                }
            }
            String taskCodeUrl = projectBaseUrl + "/" + projectCode + DS_TASK_CODE_URL;

            long taskCode = dolphinScheduleOperator.genTaskCode(taskCodeUrl, token);
            LOGGER.info("Generate task code for process definition success, task code: {}", taskCode);

            long offset = DolphinScheduleUtils.calculateOffset(scheduleInfo);
            processDefCode =
                    dolphinScheduleOperator.createProcessDef(processDefUrl, token, processName, processDesc, taskCode,
                            inlongManagerUrl, username, password, offset, scheduleInfo.getInlongGroupId());
            LOGGER.info("Create process definition success, process definition code: {}", processDefCode);

            if (dolphinScheduleOperator.releaseProcessDef(processDefUrl, processDefCode, token, DS_ONLINE_STATE)) {
                LOGGER.info("Release process definition success, release status: {}", DS_ONLINE_STATE);

                int scheduleId = dolphinScheduleOperator.createScheduleForProcessDef(scheduleUrl, processDefCode, token,
                        scheduleInfo);
                LOGGER.info("Create schedule for process definition success, schedule info: {}", scheduleInfo);

                online = dolphinScheduleOperator.onlineScheduleForProcessDef(scheduleUrl, scheduleId, token);
                LOGGER.info("Online schedule for process definition, status: {}", online);
            }

            scheduledProcessMap.putIfAbsent(processDefCode, processName);
            return online;
        } catch (Exception e) {
            LOGGER.error("Failed to handle unregister dolphin scheduler: ", e);
            throw new DolphinScheduleException(
                    String.format("Failed to handle unregister dolphin scheduler: %s", e.getMessage()));
        }
    }

    /**
     * Handle schedule unregister.
     * @param groupId group to un-register schedule info
     */
    @Override
    @VisibleForTesting
    public boolean handleUnregister(String groupId) {
        start();
        String processName = groupId + DS_DEFAULT_PROCESS_NAME;
        String processDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL;

        LOGGER.info("Dolphin Scheduler handle Unregister begin for {}, Checking process definition id uniqueness...",
                groupId);
        try {
            long processDefCode = dolphinScheduleOperator.checkAndGetUniqueId(processDefUrl, token, processName);
            if (processDefCode != 0 || scheduledProcessMap.containsKey(processDefCode)) {

                LOGGER.info("Deleting process definition, process definition id: {}", processDefCode);
                if (dolphinScheduleOperator.releaseProcessDef(processDefUrl, processDefCode, token, DS_OFFLINE_STATE)) {

                    dolphinScheduleOperator.deleteProcessDef(processDefUrl, token, processDefCode);
                    scheduledProcessMap.remove(processDefCode);
                    LOGGER.info("Process definition deleted");
                }
            }
            LOGGER.info("Un-registered dolphin schedule info for {}", groupId);
            return !scheduledProcessMap.containsKey(processDefCode);
        } catch (Exception e) {
            LOGGER.error("Failed to handle unregister dolphin scheduler: ", e);
            throw new DolphinScheduleException(
                    String.format("Failed to handle unregister dolphin scheduler: %s", e.getMessage()));
        }
    }

    /**
     * Handle schedule update.
     * @param scheduleInfo schedule info to update
     */
    @Override
    @VisibleForTesting
    public boolean handleUpdate(ScheduleInfo scheduleInfo) {
        LOGGER.info("Update dolphin schedule info for {}", scheduleInfo.getInlongGroupId());
        try {
            return handleUnregister(scheduleInfo.getInlongGroupId()) && handleRegister(scheduleInfo);
        } catch (Exception e) {
            LOGGER.error("Failed to handle update dolphin scheduler: ", e);
            throw new DolphinScheduleException(
                    String.format("Failed to handle update dolphin scheduler: %s", e.getMessage()));
        }
    }

    /**
     * stop and delete all process definition in DolphinScheduler
     * remove all process stored in scheduledProcessMap
     * delete project for inlong-group-id in DolphinScheduler
     */
    @Override
    public void stop() {
        LOGGER.info("Stopping dolphin scheduler engine...");
        String processDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL;
        try {

            String queryProcessDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL + DS_PROCESS_QUERY_URL;
            Map<Long, String> allProcessDef = dolphinScheduleOperator.queryAllProcessDef(queryProcessDefUrl, token);

            for (Long processDefCode : allProcessDef.keySet()) {

                LOGGER.info("delete process definition id: {}", processDefCode);
                dolphinScheduleOperator.releaseProcessDef(processDefUrl, processDefCode, token, DS_OFFLINE_STATE);
                dolphinScheduleOperator.deleteProcessDef(processDefUrl, token, processDefCode);
                scheduledProcessMap.remove(processDefCode);
            }

            dolphinScheduleOperator.deleteProject(projectBaseUrl, token, projectCode);
            LOGGER.info("Dolphin scheduler engine stopped");

        } catch (Exception e) {
            LOGGER.error("Failed to stop dolphin scheduler: ", e);
            throw new DolphinScheduleException(String.format("Failed to stop dolphin scheduler: %s", e.getMessage()));
        }
    }

}
