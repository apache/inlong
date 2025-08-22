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
import org.apache.inlong.agent.constant.FetcherConstants;
import org.apache.inlong.agent.pojo.COSTask.COSTaskConfig;
import org.apache.inlong.agent.pojo.FileTask.FileTaskConfig;
import org.apache.inlong.agent.pojo.SQLTask.SQLTaskConfig;
import org.apache.inlong.common.enums.TaskStateEnum;
import org.apache.inlong.common.enums.TaskTypeEnum;
import org.apache.inlong.common.pojo.agent.DataConfig;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

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
        AgentConfiguration.getAgentConf().set(FetcherConstants.AGENT_MANAGER_ADDR, "");
        AgentConfiguration.getAgentConf().set(AgentConstants.AGENT_LOCAL_IP, "127.0.0.1");
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

    public TaskProfile getFileTaskProfile(int taskId, String pattern, String dataContentStyle, boolean retry,
            String startTime, String endTime,
            TaskStateEnum state, String cycleUnit, String timeZone, List<String> filterStreams) {
        DataConfig dataConfig = getFileDataConfig(taskId, pattern, dataContentStyle, retry, startTime, endTime,
                state, cycleUnit, timeZone,
                filterStreams);
        TaskProfile profile = TaskProfile.convertToTaskProfile(dataConfig);
        return profile;
    }

    private DataConfig getFileDataConfig(int taskId, String pattern, String dataContentStyle, boolean retry,
            String startTime, String endTime, TaskStateEnum state, String cycleUnit, String timeZone,
            List<String> filterStreams) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setInlongGroupId("testGroupId");
        dataConfig.setInlongStreamId("testStreamId");
        dataConfig.setDataReportType(1);
        dataConfig.setTaskType(TaskTypeEnum.FILE.getType());
        dataConfig.setTaskId(taskId);
        dataConfig.setTimeZone(timeZone);
        dataConfig.setState(state.ordinal());
        FileTaskConfig fileTaskConfig = new FileTaskConfig();
        fileTaskConfig.setPattern(pattern);
        fileTaskConfig.setTimeOffset("0d");
        // GMT-8:00 same with Asia/Shanghai
        fileTaskConfig.setMaxFileCount(100);
        fileTaskConfig.setCycleUnit(cycleUnit);
        fileTaskConfig.setRetry(retry);
        fileTaskConfig.setDataTimeFrom(startTime);
        fileTaskConfig.setDataTimeTo(endTime);
        // mix: login|87601|968|67826|23579 or login|a=b&c=d&x=y&asdf
        fileTaskConfig.setDataContentStyle(dataContentStyle);
        fileTaskConfig.setDataSeparator("|");
        fileTaskConfig.setFilterStreams(filterStreams);
        dataConfig.setExtParams(GSON.toJson(fileTaskConfig));
        return dataConfig;
    }

    public TaskProfile getCOSTaskProfile(int taskId, String pattern, String contentStyle, boolean retry,
            String startTime, String endTime,
            TaskStateEnum state, String cycleUnit, String timeZone, List<String> filterStreams) {
        DataConfig dataConfig = getCOSDataConfig(taskId, pattern, contentStyle, retry, startTime, endTime,
                state, cycleUnit, timeZone,
                filterStreams);
        TaskProfile profile = TaskProfile.convertToTaskProfile(dataConfig);
        return profile;
    }

    private DataConfig getCOSDataConfig(int taskId, String pattern, String contentStyle, boolean retry,
            String startTime, String endTime, TaskStateEnum state, String cycleUnit, String timeZone,
            List<String> filterStreams) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setInlongGroupId("testGroupId");
        dataConfig.setInlongStreamId("testStreamId");
        dataConfig.setDataReportType(1);
        dataConfig.setTaskType(TaskTypeEnum.COS.getType());
        dataConfig.setTaskId(taskId);
        dataConfig.setTimeZone(timeZone);
        dataConfig.setState(state.ordinal());
        COSTaskConfig cosTaskConfig = new COSTaskConfig();
        cosTaskConfig.setBucketName("testBucket");
        cosTaskConfig.setCredentialsId("testSecretId");
        cosTaskConfig.setCredentialsKey("testSecretKey");
        cosTaskConfig.setRegion("testRegion");
        cosTaskConfig.setPattern(pattern);
        cosTaskConfig.setTimeOffset("0d");
        // GMT-8:00 same with Asia/Shanghai
        cosTaskConfig.setMaxFileCount(100);
        cosTaskConfig.setCycleUnit(cycleUnit);
        cosTaskConfig.setRetry(retry);
        cosTaskConfig.setDataTimeFrom(startTime);
        cosTaskConfig.setDataTimeTo(endTime);
        // mix: login|87601|968|67826|23579 or login|a=b&c=d&x=y&asdf
        cosTaskConfig.setContentStyle(contentStyle);
        cosTaskConfig.setDataSeparator("|");
        cosTaskConfig.setFilterStreams(filterStreams);
        dataConfig.setExtParams(GSON.toJson(cosTaskConfig));
        return dataConfig;
    }

    public TaskProfile getSQLTaskProfile(int taskId, String sql, String contentStyle, boolean retry,
            String startTime, String endTime, TaskStateEnum state, String cycleUnit, String timeZone) {
        DataConfig dataConfig = getSQLDataConfig(taskId, sql, contentStyle, retry, startTime, endTime,
                state, cycleUnit, timeZone);
        TaskProfile profile = TaskProfile.convertToTaskProfile(dataConfig);
        return profile;
    }

    private DataConfig getSQLDataConfig(int taskId, String sql, String contentStyle, boolean retry,
            String startTime, String endTime, TaskStateEnum state, String cycleUnit, String timeZone) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setInlongGroupId("testGroupId");
        dataConfig.setInlongStreamId("testStreamId");
        dataConfig.setDataReportType(1);
        dataConfig.setTaskType(TaskTypeEnum.SQL.getType());
        dataConfig.setTaskId(taskId);
        dataConfig.setTimeZone(timeZone);
        dataConfig.setState(state.ordinal());
        SQLTaskConfig sqlTaskConfig = new SQLTaskConfig();
        sqlTaskConfig.setUsername("testUserName");
        sqlTaskConfig.setJdbcPassword("testPassword");
        sqlTaskConfig.setSql(sql);
        sqlTaskConfig.setTimeOffset("0d");
        // GMT-8:00 same with Asia/Shanghai
        sqlTaskConfig.setMaxInstanceCount(100);
        sqlTaskConfig.setCycleUnit(cycleUnit);
        sqlTaskConfig.setRetry(retry);
        sqlTaskConfig.setDataTimeFrom(startTime);
        sqlTaskConfig.setDataTimeTo(endTime);
        dataConfig.setExtParams(GSON.toJson(sqlTaskConfig));
        return dataConfig;
    }
}
