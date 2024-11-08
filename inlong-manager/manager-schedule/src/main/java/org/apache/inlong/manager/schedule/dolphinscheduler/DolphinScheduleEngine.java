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

    @Value("${server.host:127.0.0.1}")
    private String host;

    @Value("${server.port:8083}")
    private int port;

    @Value("${default.admin.user:admin}")
    private String username;

    @Value("${default.admin.password:inlong}")
    private String password;

    @Value("${inlong.schedule.dolphinscheduler.url:}")
    private String dolphinUrl;

    @Value("${inlong.schedule.dolphinscheduler.token:}")
    private String token;

    private long projectCode;
    private final String projectBaseUrl;
    private final DolphinScheduleUtils dsUtils;
    private final Map<Long, String> scheduledProcessMap;

    public DolphinScheduleEngine(String host, int port, String username, String password, String dolphinUrl,
            String token) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.dolphinUrl = dolphinUrl;
        this.token = token;
        this.projectBaseUrl = dolphinUrl + DS_PROJECT_URL;
        try {
            LOGGER.info("Dolphin Scheduler engine http client initialized");
            this.dsUtils = new DolphinScheduleUtils();
            this.scheduledProcessMap = new ConcurrentHashMap<>();
        } catch (Exception e) {
            throw new DolphinScheduleException("Failed to init dolphin scheduler ", e);
        }
    }

    public DolphinScheduleEngine() {
        this.projectBaseUrl = dolphinUrl + DS_PROJECT_URL;
        try {
            LOGGER.info("Dolphin Scheduler engine http client initialized");
            this.dsUtils = new DolphinScheduleUtils();
            this.scheduledProcessMap = new ConcurrentHashMap<>();
        } catch (Exception e) {
            throw new DolphinScheduleException("Failed to init dolphin scheduler ", e);
        }
    }

    /**
     * check if there already exists a project for inlong offline schedule
     * if no then build a new project for inlong-group-id in DolphinScheduler
     */
    @Override
    public void start() {
        LOGGER.info("Starting dolphin scheduler engine");
        LOGGER.info("Checking project exists...");
        long code = dsUtils.checkAndGetUniqueId(projectBaseUrl, token, DS_DEFAULT_PROJECT_NAME);
        if (code != 0) {
            LOGGER.info("Project exists, project code: {}", code);
            this.projectCode = code;

            LOGGER.info("Starting synchronize existing process definition");
            String queryProcessDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL + DS_PROCESS_QUERY_URL;
            scheduledProcessMap.putAll(dsUtils.queryAllProcessDef(queryProcessDefUrl, token));

        } else {
            LOGGER.info("There is no inlong offline project exists, default project will be created");
            this.projectCode =
                    dsUtils.creatNewProject(projectBaseUrl, token, DS_DEFAULT_PROJECT_NAME, DS_DEFAULT_PROJECT_DESC);
        }
    }

    /**
     * Handle schedule register.
     * @param scheduleInfo schedule info to register
     */
    @Override
    @VisibleForTesting
    public boolean handleRegister(ScheduleInfo scheduleInfo) {
        String processDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL;
        String scheduleUrl = projectBaseUrl + "/" + projectCode + DS_SCHEDULE_URL;
        String processName = scheduleInfo.getInlongGroupId() + DS_DEFAULT_PROCESS_NAME;
        String processDesc = DS_DEFAULT_PROCESS_DESC + scheduleInfo.getInlongGroupId();

        LOGGER.info("Dolphin Scheduler handle register begin for {}", scheduleInfo.getInlongGroupId());
        LOGGER.info("Checking process definition id uniqueness...");
        try {
            long processDefCode = dsUtils.checkAndGetUniqueId(processDefUrl, token, processName);

            boolean online = false;
            if (processDefCode != 0 || scheduledProcessMap.containsKey(processDefCode)) {

                // process definition already exists, delete and rebuild
                LOGGER.info("Process definition exists, process definition id: {}, deleting...", processDefCode);
                if (dsUtils.releaseProcessDef(processDefUrl, processDefCode, token, DS_OFFLINE_STATE)) {
                    dsUtils.deleteProcessDef(processDefUrl, token, processDefCode);
                    scheduledProcessMap.remove(processDefCode);
                }
            }
            String taskCodeUrl = projectBaseUrl + "/" + projectCode + DS_TASK_CODE_URL;

            long taskCode = dsUtils.genTaskCode(taskCodeUrl, token);
            LOGGER.info("Generate task code for process definition success, task code: {}", taskCode);

            long offset = dsUtils.calculateOffset(scheduleInfo);
            processDefCode =
                    dsUtils.createProcessDef(processDefUrl, token, processName, processDesc, taskCode, host, port,
                            username, password, offset, scheduleInfo.getInlongGroupId());
            LOGGER.info("Create process definition success, process definition code: {}", processDefCode);

            if (dsUtils.releaseProcessDef(processDefUrl, processDefCode, token, DS_ONLINE_STATE)) {
                LOGGER.info("Release process definition success, release status: {}", DS_ONLINE_STATE);

                int scheduleId = dsUtils.createScheduleForProcessDef(scheduleUrl, processDefCode, token, scheduleInfo);
                LOGGER.info("Create schedule for process definition success, schedule info: {}", scheduleInfo);

                online = dsUtils.onlineScheduleForProcessDef(scheduleUrl, scheduleId, token);
                LOGGER.info("Online schedule for process definition, status: {}", online);
            }

            scheduledProcessMap.putIfAbsent(processDefCode, processName);
            return online;
        } catch (Exception e) {
            throw new DolphinScheduleException("Failed to handle unregister dolphin scheduler", e);
        }
    }

    /**
     * Handle schedule unregister.
     * @param groupId group to un-register schedule info
     */
    @Override
    @VisibleForTesting
    public boolean handleUnregister(String groupId) {
        String processName = groupId + DS_DEFAULT_PROCESS_NAME;
        String processDefUrl = projectBaseUrl + "/" + projectCode + DS_PROCESS_URL;

        LOGGER.info("Dolphin Scheduler handle Unregister begin for {}", groupId);
        LOGGER.info("Checking process definition id uniqueness...");
        try {
            long processDefCode = dsUtils.checkAndGetUniqueId(processDefUrl, token, processName);
            if (processDefCode != 0 || scheduledProcessMap.containsKey(processDefCode)) {

                LOGGER.info("Deleting process definition, process definition id: {}", processDefCode);
                if (dsUtils.releaseProcessDef(processDefUrl, processDefCode, token, DS_OFFLINE_STATE)) {

                    dsUtils.deleteProcessDef(processDefUrl, token, processDefCode);
                    scheduledProcessMap.remove(processDefCode);
                    LOGGER.info("Process definition deleted");
                }
            }
            LOGGER.info("Un-registered dolphin schedule info for {}", groupId);
            return !scheduledProcessMap.containsKey(processDefCode);
        } catch (Exception e) {
            throw new DolphinScheduleException("Failed to handle unregister dolphin scheduler", e);
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
            throw new DolphinScheduleException("Failed to handle update dolphin scheduler", e);
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
            Map<Long, String> allProcessDef = dsUtils.queryAllProcessDef(queryProcessDefUrl, token);

            for (Long processDefCode : allProcessDef.keySet()) {

                LOGGER.info("delete process definition id: {}", processDefCode);
                dsUtils.releaseProcessDef(processDefUrl, processDefCode, token, DS_OFFLINE_STATE);
                dsUtils.deleteProcessDef(processDefUrl, token, processDefCode);
                scheduledProcessMap.remove(processDefCode);
            }

            dsUtils.deleteProject(projectBaseUrl, token, projectCode);
            LOGGER.info("Dolphin scheduler engine stopped");

        } catch (Exception e) {
            throw new DolphinScheduleException("Failed to stop dolphin scheduler", e);
        }
    }

}
