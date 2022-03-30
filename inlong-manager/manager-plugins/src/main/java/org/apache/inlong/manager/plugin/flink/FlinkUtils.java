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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.inlong.manager.plugin.flink.dto.LoginConf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class FlinkUtils {
    private static final String RESOURCE_PRE = "hive-";
    public static final String BASE_DIRECTORY = "config";
    private static final Pattern numberPattern = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)");

    /**
     */
    public static final List<String> FLINK_VERSION_COLLECTION = Arrays.asList("Flink-1.13");

    /**
     * @param supportedFlink
     * @return
     */
    public static String getLatestFlinkVersion(String [] supportedFlink) {
        if (Objects.isNull(supportedFlink)) {
            return null;
        }
        Arrays.sort(supportedFlink, Collections.reverseOrder());
        String latestFinkVersion = null;
        for (int i = 0; i < supportedFlink.length; i++) {
            String flinkVersion = supportedFlink[i];
            latestFinkVersion = FLINK_VERSION_COLLECTION.stream()
                            .filter(v -> v.equals(flinkVersion)).findFirst().orElse(null);
            if (Objects.nonNull(latestFinkVersion)) {
                return latestFinkVersion;
            }
        }
        return latestFinkVersion;
    }

    /**
     * @param name
     * @return
     */
    public static String getResourceName(String name) {
        return Constants.INLONG + name;
    }

    /**
     * @param name
     * @return
     */
    public static String getConfigDirectory(String name) {
        return BASE_DIRECTORY + File.separator + name;
    }

    /**
     * @param jobId
     * @return
     */
    public static String getConfigPath(String jobId, String fileName) {
        return getConfigDirectory(jobId) + File.separator + fileName;
    }

    /**
     * @param configJobDirectory
     * @param configFileName
     * @param content
     * @return
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

    /**
     * sort_url to address&port
     * @param endpoint
     * @return
     */
    @Deprecated
    public static LoginConf translateFromEndpont(String endpoint) {
        LoginConf loginConf = new LoginConf();
        try {
            Matcher matcher = numberPattern.matcher(endpoint);
            while (matcher.find()) {
                loginConf.setRestAddress(matcher.group(1));
                loginConf.setRestPort(Integer.valueOf(matcher.group(2)));
                return loginConf;
            }
        } catch (Exception e) {
            log.error("fetch addres:port fail", e.getMessage());
        }
        return loginConf;
    }

    public static boolean deleteConfigFile(String name) {
        String configDirectory = getConfigDirectory(name);
        File file = new File(configDirectory);
        if (file.exists()) {
            try {
                FileUtils.deleteDirectory(file);
            } catch (IOException e) {
                log.error("delete %s failed", configDirectory);
                return false;
            }
        }
        return true;
    }
}
