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

package org.apache.inlong.manager.plugin.flink;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.plugin.flink.enums.ConnectorJarType;
import org.apache.inlong.manager.plugin.flink.enums.TaskCommitType;
import org.apache.inlong.manager.plugin.util.FlinkUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Flink task operation, such restart or stop flink job.
 */
@Slf4j
public class FlinkOperation {

    private static final Integer TRY_MAX_TIMES = 60;
    private static final String CONFIG_FILE = "application.properties";
    private static final String CONNECTOR_DIR_KEY = "sort.connector.dir";
    private static final String JOB_TERMINATED_MSG = "the job not found by id %s, "
            + "or task already terminated or savepoint path is null";
    private static final String INLONG_MANAGER = "inlong-manager";
    private static final String INLONG_SORT = "inlong-sort";
    private static final String SORT_JAR_PATTERN = "^sort-dist.*jar$";
    private static final String CONNECTOR_JAR_PATTERN = "^sort-connector-(?i)(%s).*jar$";
    private static final String ALL_CONNECTOR_JAR_PATTERN = "^sort-connector-.*jar$";
    private static Properties properties;

    private static class FlinkOperationHolder {

        private static final FlinkOperation INSTANCE = new FlinkOperation();
    }

    public static FlinkOperation getInstance() {
        return FlinkOperationHolder.INSTANCE;
    }

    /**
     * Get sort connector directory
     */
    private static String getConnectorDir(String parent) throws IOException {
        if (properties == null) {
            properties = new Properties();
            String path = Thread.currentThread().getContextClassLoader().getResource("").getPath() + CONFIG_FILE;
            try (InputStream inputStream = new BufferedInputStream(Files.newInputStream(Paths.get(path)))) {
                properties.load(inputStream);
            }
        }
        return properties.getProperty(CONNECTOR_DIR_KEY, Paths.get(parent, INLONG_SORT, "connectors").toString());
    }

    /**
     * Get Sort connector jar patterns from the Flink info.
     */
    private String getConnectorJarPattern(String dataSourceType) {
        ConnectorJarType connectorJarType = ConnectorJarType.getInstance(dataSourceType);
        return connectorJarType == null
                ? ALL_CONNECTOR_JAR_PATTERN
                : String.format(CONNECTOR_JAR_PATTERN, connectorJarType.getConnectorType());

    }

    /**
     * Restart the Flink job.
     */
    public void restart(FlinkInfo flinkInfo) throws Exception {
        boolean terminated = isNullOrTerminated(flinkInfo);
        if (terminated) {
            String message = String.format("restart job failed, as " + JOB_TERMINATED_MSG, flinkInfo.getJobId());
            log.error(message);
            throw new Exception(message);
        }

        Future<?> future = TaskRunService.submit(
                new IntegrationTaskRunner(flinkInfo, TaskCommitType.RESTART.getCode()));
        future.get();
    }

    /**
     * Start the Flink job, if the job id was not empty, restore it.
     */
    public void start(FlinkInfo flinkInfo) throws Exception {
        try {
            // Start a new task without savepoint
            if (StringUtils.isEmpty(flinkInfo.getJobId())) {
                IntegrationTaskRunner taskRunner = new IntegrationTaskRunner(flinkInfo,
                        TaskCommitType.START_NOW.getCode());
                Future<?> future = TaskRunService.submit(taskRunner);
                future.get();
            } else {
                // Restore an old task with savepoint
                boolean noSavepoint =
                        isNullOrTerminated(flinkInfo) || StringUtils.isEmpty(flinkInfo.getSavepointPath());
                if (noSavepoint) {
                    String message =
                            String.format("restore job failed, as " + JOB_TERMINATED_MSG, flinkInfo.getJobId());
                    log.error(message);
                    throw new Exception(message);
                }

                IntegrationTaskRunner taskRunner = new IntegrationTaskRunner(flinkInfo,
                        TaskCommitType.RESUME.getCode());
                Future<?> future = TaskRunService.submit(taskRunner);
                future.get();
            }
        } catch (Exception e) {
            log.warn("submit flink job failed for {}", flinkInfo, e);
            throw new Exception("submit flink job failed: " + e.getMessage());
        }
    }

    private void checkNodeIds(String dataflow) throws Exception {
        JsonNode relations = JsonUtils.parseTree(dataflow).get(InlongConstants.STREAMS)
                .get(0).get(InlongConstants.RELATIONS);
        List<Pair<List<String>, List<String>>> nodeIdsPairList = new ArrayList<>();
        for (int i = 0; i < relations.size(); i++) {
            List<String> inputIds = new ArrayList<>(
                    JsonUtils.OBJECT_MAPPER.convertValue(relations.get(i).get(InlongConstants.INPUTS),
                            new TypeReference<List<String>>() {
                            }));
            if (CollectionUtils.isEmpty(inputIds)) {
                String message = String.format("input nodeId %s cannot be empty", inputIds);
                log.error(message);
                throw new Exception(message);
            }

            List<String> outputIds = new ArrayList<>(
                    JsonUtils.OBJECT_MAPPER.convertValue(relations.get(i).get(InlongConstants.OUTPUTS),
                            new TypeReference<List<String>>() {
                            }));
            if (CollectionUtils.isEmpty(outputIds)) {
                String message = String.format("output nodeId %s cannot be empty", outputIds);
                log.error(message);
                throw new Exception(message);
            }

            if (!Collections.disjoint(inputIds, outputIds)) {
                String message = String.format("input nodeId %s cannot be equal to output nodeId %s",
                        inputIds, outputIds);
                log.error(message);
                throw new Exception(message);
            }
            nodeIdsPairList.add(Pair.of(inputIds, outputIds));
        }

        if (nodeIdsPairList.size() > 1) {
            List<String> allNodeIds = new ArrayList<>(nodeIdsPairList.get(0).getLeft());
            allNodeIds.addAll(nodeIdsPairList.get(0).getRight());
            for (int i = 1; i < relations.size(); i++) {
                if (!Collections.disjoint(allNodeIds, nodeIdsPairList.get(i).getLeft())) {
                    String message = String.format("input nodeId %s already exists ", nodeIdsPairList.get(i).getLeft());
                    log.error(message);
                    throw new Exception(message);
                }
                allNodeIds.addAll(nodeIdsPairList.get(i).getLeft());
            }
        }
    }

    /**
     * Build Flink local path.
     */
    public void genPath(FlinkInfo flinkInfo, String dataflow) throws Exception {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        log.info("gen path from {}", path);

        int index = path.indexOf(INLONG_MANAGER);
        if (index == -1) {
            throw new Exception(INLONG_MANAGER + " path not found in " + path);
        }

        path = path.substring(0, path.lastIndexOf(File.separator));
        String startPath = path.substring(0, index);
        String basePath = startPath + INLONG_SORT;
        File file = new File(basePath);
        if (!file.exists()) {
            String message = String.format("file path [%s] not found", basePath);
            log.error(message);
            throw new Exception(message);
        }

        String jarPath = FlinkUtils.findFile(basePath, SORT_JAR_PATTERN);
        flinkInfo.setLocalJarPath(jarPath);
        log.info("get sort jar path success, path: {}", jarPath);

        List<String> nodeTypes = new ArrayList<>();
        if (StringUtils.isNotEmpty(dataflow)) {
            // TODO Temporarily remove this method. The method of checking node needs to be modified.
            // checkNodeIds(dataflow);
            JsonNode nodes = JsonUtils.parseTree(dataflow).get(InlongConstants.STREAMS)
                    .get(0).get(InlongConstants.NODES);
            List<String> types = JsonUtils.OBJECT_MAPPER.convertValue(nodes,
                    new TypeReference<List<Map<String, Object>>>() {
                    }).stream().map(s -> s.get(InlongConstants.NODE_TYPE).toString()).collect(Collectors.toList());
            nodeTypes.addAll(types);
        }

        String connectorDir = getConnectorDir(startPath);
        Set<String> connectorPaths = nodeTypes.stream().filter(
                s -> s.endsWith(InlongConstants.LOAD) || s.endsWith(InlongConstants.EXTRACT)).map(
                        s -> FlinkUtils.listFiles(connectorDir, getConnectorJarPattern(s), -1))
                .flatMap(Collection::stream).collect(Collectors.toSet());

        if (CollectionUtils.isEmpty(connectorPaths)) {
            String message = String.format("no sort connectors found in %s", connectorDir);
            log.error(message);
            throw new RuntimeException(message);
        }

        flinkInfo.setConnectorJarPaths(new ArrayList<>(connectorPaths));
        log.info("get sort connector paths success, paths: {}", connectorPaths);

        if (FlinkUtils.writeConfigToFile(path, flinkInfo.getJobName(), dataflow)) {
            flinkInfo.setLocalConfPath(path + File.separator + flinkInfo.getJobName());
        } else {
            String message = String.format("write dataflow to %s failed", path);
            log.error(message + ", dataflow: {}", dataflow);
            throw new Exception(message);
        }
    }

    /**
     * Stop the Flink job.
     */
    public void stop(FlinkInfo flinkInfo) throws Exception {
        boolean terminated = isNullOrTerminated(flinkInfo);
        if (terminated) {
            String message = String.format("stop job failed, as " + JOB_TERMINATED_MSG, flinkInfo.getJobId());
            log.error(message);
            throw new Exception(message);
        }

        Future<?> future = TaskRunService.submit(
                new IntegrationTaskRunner(flinkInfo, TaskCommitType.STOP.getCode()));
        future.get();
    }

    /**
     * Delete the Flink job
     */
    public void delete(FlinkInfo flinkInfo) throws Exception {
        String jobId = flinkInfo.getJobId();
        JobDetailsInfo jobDetailsInfo = FlinkService.getInstance().getJobDetail(flinkInfo);
        if (jobDetailsInfo == null) {
            throw new Exception(String.format("delete job failed as the job [%s] not found", jobId));
        }

        JobStatus jobStatus = jobDetailsInfo.getJobStatus();
        if (jobStatus != null && jobStatus.isTerminalState()) {
            String stateName = jobStatus.isGloballyTerminalState() ? "globally" : "locally";
            String message = String.format("unsupported delete job [%s] as it was [%s] terminated", jobId, stateName);
            throw new Exception(message);
        }

        Future<?> future = TaskRunService.submit(
                new IntegrationTaskRunner(flinkInfo, TaskCommitType.DELETE.getCode()));
        future.get();
    }

    /**
     * Status of Flink job.
     */
    public void pollJobStatus(FlinkInfo flinkInfo, JobStatus expectStatus) throws Exception {
        if (flinkInfo.isException()) {
            throw new BusinessException("startup failed: " + flinkInfo.getExceptionMsg());
        }
        String jobId = flinkInfo.getJobId();
        if (StringUtils.isBlank(jobId)) {
            log.error("job id cannot empty for {}", flinkInfo);
            throw new Exception("job id cannot empty");
        }
        int retryTimes = 0;
        while (retryTimes <= TRY_MAX_TIMES) {
            try {
                JobDetailsInfo jobDetailsInfo = FlinkService.getInstance().getJobDetail(flinkInfo);
                if (jobDetailsInfo == null) {
                    log.error("job detail not found by {}", jobId);
                    throw new Exception(String.format("job detail not found by %s", jobId));
                }

                JobStatus jobStatus = jobDetailsInfo.getJobStatus();
                if (jobStatus.isTerminalState() && expectStatus != JobStatus.CANCELED) {
                    log.error("job was terminated for {}, exception: {}", jobId, flinkInfo.getExceptionMsg());
                    throw new Exception("job was terminated for " + jobId);
                }

                if (jobStatus == expectStatus) {
                    log.info("job status is {} for {}", jobStatus, jobId);
                    break;
                }
                log.info("job status is {} for {}", jobStatus, jobId);
            } catch (Exception e) {
                log.error("poll job status error for {}, exception: ", flinkInfo, e);
            }
            TimeUnit.SECONDS.sleep(5);
            retryTimes++;
        }
    }

    /**
     * Check whether the job was terminated by the given job id.
     */
    private boolean isNullOrTerminated(FlinkInfo flinkInfo) throws Exception {
        JobDetailsInfo jobDetailsInfo = FlinkService.getInstance().getJobDetail(flinkInfo);
        boolean terminated = jobDetailsInfo == null || jobDetailsInfo.getJobStatus() == null;
        if (terminated) {
            log.warn("job detail or job status was null for [{}]", flinkInfo.getJobId());
            return true;
        }

        terminated = jobDetailsInfo.getJobStatus().isTerminalState();
        log.warn("job terminated state was [{}] for [{}]", terminated, jobDetailsInfo);
        return terminated;
    }

}
