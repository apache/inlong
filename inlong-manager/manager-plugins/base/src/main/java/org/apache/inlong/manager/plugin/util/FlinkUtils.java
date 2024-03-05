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

import org.apache.inlong.manager.plugin.flink.dto.FlinkConfig;
import org.apache.inlong.manager.plugin.flink.enums.Constants;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
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
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.inlong.common.constant.Constants.METRICS_AUDIT_PROXY_HOSTS_KEY;
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
        flinkConfig.setAuditProxyHosts(properties.getProperty(METRICS_AUDIT_PROXY_HOSTS_KEY));
        flinkConfig.setVersion(properties.getProperty(FLINK_VERSION));
        return flinkConfig;
    }
}
