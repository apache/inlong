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

package org.apache.inlong.manager.plugin.util;

import org.apache.inlong.common.bounded.Boundaries;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.plugin.flink.FlinkOperation;
import org.apache.inlong.manager.plugin.flink.dto.FlinkConfig;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.plugin.flink.enums.Constants;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.stream.InlongStreamExtInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.workflow.event.ListenerResult;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.consts.InlongConstants.RUNTIME_EXECUTION_MODE_BATCH;
import static org.apache.inlong.manager.common.consts.InlongConstants.RUNTIME_EXECUTION_MODE_STREAM;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.ADDRESS;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.DRAIN;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.FLINK_VERSION;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.JOB_MANAGER_PORT;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.PARALLELISM;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.PORT;
import static org.apache.inlong.manager.plugin.flink.enums.Constants.SAVEPOINT_DIRECTORY;

/**
 * Util of flink.
 */
@Slf4j
public class FlinkUtils {

    public static final String BASE_DIRECTORY = "config";
    private static final String DEFAULT_PLUGINS = "plugins";
    private static final String FILE_PREFIX = "file://";
    private static final String DEFAULT_CONFIG_FILE = "flink-sort-plugin.properties";

    /**
     * print exception
     */
    public static String getExceptionStackMsg(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        throwable.printStackTrace(new PrintWriter(stringWriter, true));
        return stringWriter.getBuffer().toString();
    }

    /**
     * fetch sort-single-tenant jar path
     *
     * @param baseDirName base directory name.
     * @param pattern pattern of file
     * @return sort-single-tenant jar path
     */
    public static String findFile(String baseDirName, String pattern) {
        List<String> files = listFiles(baseDirName, pattern, 1);
        if (CollectionUtils.isEmpty(files)) {
            return null;
        }
        return files.get(0);
    }

    /**
     * fetch target file path
     *
     * @param baseDirName base directory name.
     * @param pattern pattern of file
     * @return matched files
     */
    public static List<String> listFiles(String baseDirName, String pattern, int limit) {
        List<String> result = new ArrayList<>();

        File baseDir = new File(baseDirName);
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            log.error("baseDirName find fail: {}", baseDirName);
            return result;
        }
        String tempName;
        File tempFile;
        File[] files = baseDir.listFiles();
        if (files == null || files.length == 0) {
            log.info("baseDirName is empty");
            return result;
        }
        for (File file : files) {
            tempFile = file;
            tempName = tempFile.getName();
            Pattern jarPathPattern = Pattern.compile(pattern);
            Matcher matcher = jarPathPattern.matcher(tempName);
            boolean matches = matcher.matches();
            if (matches) {
                result.add(tempFile.getAbsoluteFile().toString());
            }
            if (limit > 0 && result.size() >= limit) {
                return result;
            }
        }
        return result;
    }

    /**
     * getConfigDirectory
     *
     * @param name config file name
     * @return config file directory
     */
    public static String getConfigDirectory(String name) {
        return BASE_DIRECTORY + File.separator + name;
    }

    /**
     * writeConfigToFile
     *
     * @param configJobDirectory job config directory
     * @param configFileName config file name
     * @param content contents of the file to be written
     * @return whether success
     */
    public static boolean writeConfigToFile(String configJobDirectory, String configFileName, String content) {
        File file = new File(configJobDirectory);
        if (!file.exists()) {
            file.mkdirs();
        }
        String filePath = configJobDirectory + File.separator + configFileName;
        try {
            FileWriter fileWriter = new FileWriter(filePath);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(content);
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            log.error("saveConfigToLocal failed", e);
            return false;
        }
        return true;
    }

    public static Object getFlinkClientService(Configuration configuration, FlinkConfig flinkConfig) {
        log.info("Flink version {}", flinkConfig.getVersion());

        Path pluginPath = Paths.get(DEFAULT_PLUGINS).toAbsolutePath();
        String flinkJarName = String.format(Constants.FLINK_JAR_NAME, flinkConfig.getVersion());
        String flinkClientPath = FILE_PREFIX + pluginPath + File.separator + flinkJarName;
        log.info("Start to load Flink jar: {}", flinkClientPath);

        try (URLClassLoader classLoader = new URLClassLoader(new URL[]{new URL(flinkClientPath)}, Thread.currentThread()
                .getContextClassLoader())) {
            Class<?> flinkClientService = classLoader.loadClass(Constants.FLINK_CLIENT_CLASS);
            Object flinkService = flinkClientService.getDeclaredConstructor(Configuration.class)
                    .newInstance(configuration);
            log.info("Successfully loaded Flink service");
            return flinkService;
        } catch (Exception e) {
            log.error("Failed to loaded Flink service, please check flink client jar path: {}", flinkClientPath);
            throw new RuntimeException(e);
        }
    }

    public static FlinkConfig getFlinkConfigFromFile() throws Exception {
        Path pluginPath = Paths.get(DEFAULT_PLUGINS).toAbsolutePath();
        String defaultConfigFilePath = pluginPath + File.separator + DEFAULT_CONFIG_FILE;

        log.info("Start to load Flink config from file: {}", defaultConfigFilePath);

        Properties properties = new Properties();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(defaultConfigFilePath))) {
            properties.load(bufferedReader);
        }
        FlinkConfig flinkConfig = new FlinkConfig();
        flinkConfig.setPort(Integer.valueOf(properties.getProperty(PORT)));
        flinkConfig.setAddress(properties.getProperty(ADDRESS));
        flinkConfig.setParallelism(Integer.valueOf(properties.getProperty(PARALLELISM)));
        flinkConfig.setSavepointDirectory(properties.getProperty(SAVEPOINT_DIRECTORY));
        flinkConfig.setJobManagerPort(Integer.valueOf(properties.getProperty(JOB_MANAGER_PORT)));
        flinkConfig.setDrain(Boolean.parseBoolean(properties.getProperty(DRAIN)));
        flinkConfig.setVersion(properties.getProperty(FLINK_VERSION));
        flinkConfig.setDynamicParallelismEnable(Boolean.parseBoolean(properties.getProperty(
                Constants.FLINK_DYNAMIC_PARALLELISM_ENABLE)));
        flinkConfig.setMaxMsgRatePerCore(Integer.valueOf(properties.getProperty(Constants.FLINK_MAX_MSG_RATE_PERCORE)));
        return flinkConfig;
    }

    public static ListenerResult submitFlinkJobs(String groupId, List<InlongStreamInfo> streamInfoList)
            throws Exception {
        return submitFlinkJobs(groupId, streamInfoList, false, null);
    }

    public static ListenerResult submitFlinkJobs(String groupId, List<InlongStreamInfo> streamInfoList,
            boolean isBatchJob, Boundaries boundaries) throws Exception {
        int sinkCount = streamInfoList.stream()
                .map(s -> s.getSinkList() == null ? 0 : s.getSinkList().size())
                .reduce(0, Integer::sum);
        if (sinkCount == 0) {
            log.warn("Not any sink configured for group {} and stream list {}, skip launching sort job", groupId,
                    streamInfoList.stream()
                            .map(s -> s.getInlongGroupId() + ":" + s.getName()).collect(Collectors.toList()));
            return ListenerResult.success();
        }

        List<ListenerResult> listenerResults = new ArrayList<>();
        for (InlongStreamInfo streamInfo : streamInfoList) {
            listenerResults.add(
                    FlinkUtils.submitFlinkJob(
                            streamInfo, FlinkUtils.genFlinkJobName(streamInfo), isBatchJob, boundaries));
        }

        // only one stream in group for now
        // we can return the list of ListenerResult if support multi-stream in the future
        List<ListenerResult> failedStreams = listenerResults.stream()
                .filter(t -> !t.isSuccess()).collect(Collectors.toList());
        if (failedStreams.isEmpty()) {
            return ListenerResult.success();
        }
        return ListenerResult.fail(failedStreams.get(0).getRemark());
    }

    public static ListenerResult submitFlinkJob(InlongStreamInfo streamInfo, String jobName) throws Exception {
        return submitFlinkJob(streamInfo, jobName, false, null);
    }

    public static ListenerResult submitFlinkJob(InlongStreamInfo streamInfo, String jobName,
            boolean isBatchJob, Boundaries boundaries) throws Exception {
        List<StreamSink> sinkList = streamInfo.getSinkList();
        List<String> sinkTypes = sinkList.stream().map(StreamSink::getSinkType).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(sinkList) || !SinkType.containSortFlinkSink(sinkTypes)) {
            log.warn("not any valid sink configured for groupId {} and streamId {}, reason: {},"
                    + " skip launching sort job",
                    CollectionUtils.isEmpty(sinkList) ? "no sink configured" : "no sort flink sink configured",
                    streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId());
            return ListenerResult.success();
        }
        List<InlongStreamExtInfo> extList = streamInfo.getExtList();
        log.info("stream ext info: {}", extList);
        Map<String, String> kvConf = extList.stream().filter(v -> StringUtils.isNotEmpty(v.getKeyName())
                && StringUtils.isNotEmpty(v.getKeyValue())).collect(Collectors.toMap(
                        InlongStreamExtInfo::getKeyName,
                        InlongStreamExtInfo::getKeyValue));

        String sortExtProperties = kvConf.get(InlongConstants.SORT_PROPERTIES);
        if (StringUtils.isNotEmpty(sortExtProperties)) {
            Map<String, String> result = JsonUtils.OBJECT_MAPPER.convertValue(
                    JsonUtils.OBJECT_MAPPER.readTree(sortExtProperties), new TypeReference<Map<String, String>>() {
                    });
            kvConf.putAll(result);
        }

        String dataflow = kvConf.get(InlongConstants.DATAFLOW);
        if (StringUtils.isEmpty(dataflow)) {
            String message = String.format("dataflow is empty for groupId [%s], streamId [%s]",
                    streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId());
            log.error(message);
            return ListenerResult.fail(message);
        }

        FlinkInfo flinkInfo = new FlinkInfo();
        flinkInfo.setJobName(jobName);
        String sortUrl = kvConf.get(InlongConstants.SORT_URL);
        flinkInfo.setEndpoint(sortUrl);
        flinkInfo.setInlongStreamInfoList(Collections.singletonList(streamInfo));
        if (isBatchJob) {
            flinkInfo.setRuntimeExecutionMode(RUNTIME_EXECUTION_MODE_BATCH);
            flinkInfo.setBoundaryType(boundaries.getBoundaryType().getType());
            flinkInfo.setLowerBoundary(boundaries.getLowerBound());
            flinkInfo.setUpperBoundary(boundaries.getUpperBound());
        } else {
            flinkInfo.setRuntimeExecutionMode(RUNTIME_EXECUTION_MODE_STREAM);
        }
        FlinkOperation flinkOperation = FlinkOperation.getInstance();
        try {
            flinkOperation.genPath(flinkInfo, dataflow);
            flinkOperation.start(flinkInfo);
            log.info("job submit success for groupId = {}, streamId = {}, jobId = {}",
                    streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId(), flinkInfo.getJobId());
        } catch (Exception e) {
            flinkInfo.setException(true);
            flinkInfo.setExceptionMsg(getExceptionStackMsg(e));
            flinkOperation.pollJobStatus(flinkInfo, JobStatus.RUNNING);

            String message = String.format("startup sort failed for groupId [%s], streamId [%s]",
                    streamInfo.getInlongGroupId(), streamInfo.getInlongStreamId());
            log.error(message, e);
            return ListenerResult.fail(message + e.getMessage());
        }

        saveInfo(streamInfo, InlongConstants.SORT_JOB_ID, flinkInfo.getJobId(), extList);
        flinkOperation.pollJobStatus(flinkInfo, JobStatus.RUNNING);
        return ListenerResult.success();
    }

    /**
     * Save stream ext info into list.
     */
    public static void saveInfo(InlongStreamInfo streamInfo, String keyName, String keyValue,
            List<InlongStreamExtInfo> extInfoList) {
        InlongStreamExtInfo extInfo = new InlongStreamExtInfo();
        extInfo.setInlongGroupId(streamInfo.getInlongGroupId());
        extInfo.setInlongStreamId(streamInfo.getInlongStreamId());
        extInfo.setKeyName(keyName);
        extInfo.setKeyValue(keyValue);
        extInfoList.add(extInfo);
    }

    public static String genFlinkJobName(ProcessForm processForm, InlongStreamInfo streamInfo) {
        return Constants.SORT_JOB_NAME_GENERATOR.apply(processForm) + InlongConstants.HYPHEN
                + streamInfo.getInlongStreamId();
    }

    public static String genFlinkJobName(InlongStreamInfo streamInfo) {
        return String.format(Constants.SORT_JOB_NAME_TEMPLATE, streamInfo.getInlongGroupId()) + InlongConstants.HYPHEN
                + streamInfo.getInlongStreamId();
    }
}
