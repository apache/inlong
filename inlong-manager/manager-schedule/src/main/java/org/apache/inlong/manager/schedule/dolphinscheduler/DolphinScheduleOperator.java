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
import org.apache.inlong.manager.schedule.exception.DolphinScheduleException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.UNEXPECTED_ERROR;

/**
 * DolphinScheduler operator, This class includes methods for creating, updating, and deleting projects,
 * tasks, and process definitions in DolphinScheduler.
 */
@Service
public class DolphinScheduleOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DolphinScheduleOperator.class);

    /**
     * Checks the uniqueness of a DolphinScheduler project ID based on the given search value.
     */
    public long checkAndGetUniqueId(String url, String token, String searchVal) {
        try {
            return DolphinScheduleUtils.checkAndGetUniqueId(url, token, searchVal);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in check id uniqueness: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in check id uniqueness: %s", e.getMessage()));
        }
    }

    /**
     * Creates a new project in DolphinScheduler.
     */
    public long creatProject(String url, String token, String projectName, String description) {
        try {
            return DolphinScheduleUtils.creatProject(url, token, projectName, description);
        } catch (Exception e) {
            LOGGER.error("Unexpected error while creating new project: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected error while creating new project: %s", e.getMessage()));
        }
    }

    /**
     * Query all process definition in DolphinScheduler project.
     */
    public Map<Long, String> queryAllProcessDef(String url, String token) {
        try {
            return DolphinScheduleUtils.queryAllProcessDef(url, token);
        } catch (Exception e) {
            LOGGER.error("Unexpected error while querying process definition: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected error while querying process definition: %s", e.getMessage()));
        }
    }

    /**
     * Generates a new task code in DolphinScheduler.
     */
    public long genTaskCode(String url, String token) {
        try {
            return DolphinScheduleUtils.genTaskCode(url, token);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in generating task code: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in generating task code: %s", e.getMessage()));
        }
    }

    /**
     * Creates a process definition in DolphinScheduler.
     */
    public long createProcessDef(String url, String token, String name, String desc, long taskCode, String host,
            int port, String username, String password, long offset, String groupId) {
        try {
            return DolphinScheduleUtils.createProcessDef(url, token, name, desc, taskCode, host,
                    port, username, password, offset, groupId);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in creating process definition: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in creating process definition: %s", e.getMessage()));
        }
    }

    /**
     * Releases a process definition in DolphinScheduler.
     */
    public boolean releaseProcessDef(String processDefUrl, long processDefCode, String token, String status) {
        try {
            return DolphinScheduleUtils.releaseProcessDef(processDefUrl, processDefCode, token, status);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in release process definition: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in release process definition: %s", e.getMessage()));
        }
    }

    /**
     * Create a schedule for process definition in DolphinScheduler.
     */
    public int createScheduleForProcessDef(String url, long processDefCode, String token, ScheduleInfo scheduleInfo) {
        try {
            return DolphinScheduleUtils.createScheduleForProcessDef(url, processDefCode, token,
                    scheduleInfo);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in creating schedule for process definition: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in creating schedule for process definition: %s", e.getMessage()));
        }
    }

    /**
     * Online the schedule for process definition in DolphinScheduler.
     */
    public boolean onlineScheduleForProcessDef(String scheduleUrl, int scheduleId, String token) {
        try {
            return DolphinScheduleUtils.onlineScheduleForProcessDef(scheduleUrl, scheduleId, token);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in online process definition: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in online process definition: %s", e.getMessage()));
        }
    }

    /**
     * Delete the process definition in DolphinScheduler.
     */
    public void deleteProcessDef(String processDefUrl, String token, long processDefCode) {
        try {
            DolphinScheduleUtils.delete(processDefUrl, token, processDefCode);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in deleting process definition: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in deleting process definition: %s", e.getMessage()));
        }
    }

    /**
     * Delete the project in DolphinScheduler.
     */
    public void deleteProject(String projectBaseUrl, String token, long projectCode) {
        try {
            DolphinScheduleUtils.delete(projectBaseUrl, token, projectCode);
        } catch (Exception e) {
            LOGGER.error("Unexpected wrong in deleting project definition: ", e);
            throw new DolphinScheduleException(UNEXPECTED_ERROR,
                    String.format("Unexpected wrong in deleting project definition: %s", e.getMessage()));
        }
    }

}
