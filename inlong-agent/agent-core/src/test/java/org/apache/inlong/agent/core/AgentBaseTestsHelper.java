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

package org.apache.inlong.agent.core;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.FetcherConstants;
import org.apache.inlong.agent.pojo.FileTask.FileTaskConfig;
import org.apache.inlong.common.enums.TaskStateEnum;
import org.apache.inlong.common.pojo.agent.DataConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * common environment setting up for test cases.
 */
public class AgentBaseTestsHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentBaseTestsHelper.class);
    private static final GsonBuilder gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = gsonBuilder.create();
    private final String className;
    private Path testRootDir;

    public AgentBaseTestsHelper(String className) {
        this.className = className;
    }

    public AgentBaseTestsHelper setupAgentHome() {
        testRootDir = Paths
                .get("/tmp", AgentBaseTestsHelper.class.getSimpleName(), className);
        teardownAgentHome();
        boolean result = testRootDir.toFile().mkdirs();
        LOGGER.info("try to create {}, result is {}", testRootDir, result);
        AgentConfiguration.getAgentConf().set(AgentConstants.AGENT_HOME, testRootDir.toString());
        AgentConfiguration.getAgentConf().set(FetcherConstants.AGENT_MANAGER_ADDR, "");
        AgentConfiguration.getAgentConf().set(AgentConstants.AGENT_LOCAL_IP, "127.0.0.1");
        return this;
    }

    public Path getTestRootDir() {
        return testRootDir;
    }

    public void teardownAgentHome() {
        if (testRootDir != null) {
            try {
                FileUtils.deleteDirectory(testRootDir.toFile());
            } catch (Exception ignored) {
                LOGGER.warn("deleteDirectory error ", ignored);
            }
        }
    }

    public TaskProfile getTaskProfile(int taskId, String pattern, boolean retry, String startTime, String endTime,
            TaskStateEnum state, String timeZone) {
        DataConfig dataConfig = getDataConfig(taskId, pattern, retry, startTime, endTime, state, timeZone);
        TaskProfile profile = TaskProfile.convertToTaskProfile(dataConfig);
        return profile;
    }

    private DataConfig getDataConfig(int taskId, String pattern, boolean retry, String startTime, String endTime,
            TaskStateEnum state, String timeZone) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setInlongGroupId("testGroupId");
        dataConfig.setInlongStreamId("testStreamId");
        dataConfig.setDataReportType(1);
        dataConfig.setTaskType(3);
        dataConfig.setTaskId(taskId);
        dataConfig.setTimeZone(timeZone);
        dataConfig.setState(state.ordinal());
        FileTaskConfig fileTaskConfig = new FileTaskConfig();
        fileTaskConfig.setPattern(pattern);
        fileTaskConfig.setTimeOffset("0h");
        fileTaskConfig.setMaxFileCount(100);
        fileTaskConfig.setCycleUnit("h");
        fileTaskConfig.setRetry(retry);
        fileTaskConfig.setDataTimeFrom(startTime);
        fileTaskConfig.setDataTimeTo(endTime);
        dataConfig.setExtParams(GSON.toJson(fileTaskConfig));
        return dataConfig;
    }
}
