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

package org.apache.inlong.agent.plugin;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.AgentConstants;
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
    private Path parentPath;

    public AgentBaseTestsHelper(String className) {
        this.className = className;
    }

    public AgentBaseTestsHelper setupAgentHome() {
        parentPath = Paths.get("./").toAbsolutePath();
        testRootDir = Paths
                .get("/tmp", AgentBaseTestsHelper.class.getSimpleName(), className);
        teardownAgentHome();
        boolean result = testRootDir.toFile().mkdirs();
        LOGGER.info("try to create {}, result is {}", testRootDir, result);
        AgentConfiguration.getAgentConf().set(AgentConstants.AGENT_HOME, testRootDir.toString());
        return this;
    }

    public Path getParentPath() {
        return parentPath;
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

    public TaskProfile getTaskProfile(int taskId, String pattern, boolean retry, Long startTime, Long endTime,
            TaskStateEnum state) {
        DataConfig dataConfig = getDataConfig(taskId, pattern, retry, startTime, endTime, state);
        TaskProfile profile = TaskProfile.convertToTaskProfile(dataConfig);
        return profile;
    }

    private DataConfig getDataConfig(int taskId, String pattern, boolean retry, Long startTime, Long endTime,
            TaskStateEnum state) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setInlongGroupId("testGroupId"); // 老字段 groupId
        dataConfig.setInlongStreamId("testStreamId"); // 老字段 streamId
        dataConfig.setDataReportType(1); // 老字段 reportType
        dataConfig.setTaskType(3); // 老字段 任务类型，3 代表文件采集
        dataConfig.setTaskId(taskId); // 老字段 任务 id
        dataConfig.setState(state.ordinal()); // 新增！ 任务状态 1 正常 2 暂停
        FileTaskConfig fileTaskConfig = new FileTaskConfig();
        fileTaskConfig.setPattern(pattern);// 正则
        fileTaskConfig.setTimeOffset("0d"); // 老字段 时间偏移 "-1d" 采一天前的 "-2h" 采 2 小时前的
        fileTaskConfig.setMaxFileCount(100); // 最大文件数
        fileTaskConfig.setCycleUnit("D"); // 新增！ 任务周期 "D" 天 "h" 小时
        fileTaskConfig.setRetry(retry); // 新增！ 是否补录，如果是补录任务则为 true
        fileTaskConfig.setStartTime(startTime);
        fileTaskConfig.setEndTime(endTime);
        dataConfig.setExtParams(GSON.toJson(fileTaskConfig));
        return dataConfig;
    }
}
