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

package org.apache.inlong.manager.plugin.conf;

import com.google.gson.JsonPrimitive;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.plugin.flink.FlinkConfig;
import org.apache.inlong.manager.plugin.flink.enums.BusinessExceptionDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.inlong.manager.plugin.flink.Constants.ADDRESS;
import static org.apache.inlong.manager.plugin.flink.Constants.JOB_MANAGER_PORT;
import static org.apache.inlong.manager.plugin.flink.Constants.PARALLELISM;
import static org.apache.inlong.manager.plugin.flink.Constants.PORT;
import static org.apache.inlong.manager.plugin.flink.Constants.SAVEPOINT_DIRECTORY;

/**
 * flink configuration. Only one instance in the process.
 * Basically it use properties file to store configurations.
 */
public class FlinkConfiguration  {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkConfiguration.class);

    private static final String DEFAULT_CONFIG_FILE = "flink-sort-plugin.properties";

    private final Map<String, JsonPrimitive> configStorage = new HashMap<>();

    private  FlinkConfig flinkConfig;

    /**
     * fetch DEFAULT_CONFIG_FILE full path
     * @return
     */
    private String formatPath() {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        LOGGER.info("format first path {}",path);
        String inlongManager = "inlong-manager";
        if (path.contains(inlongManager)) {
            path = path.substring(0, path.indexOf(inlongManager));
            String confPath = path + inlongManager + File.separator + "plugins" + File.separator + DEFAULT_CONFIG_FILE;
            LOGGER.info("flink-sort-plugin.properties path : {}",confPath);
            File file = new File(confPath);
            if (!file.exists()) {
                LOGGER.warn("plugins config file path:[{}] not found flink-sort-plugin.properties", confPath);
                throw new BusinessException(BusinessExceptionDesc.InternalError
                        + " not found flink-sort-plugin.properties");
            }
            return confPath;
        } else {
            throw new BusinessException(BusinessExceptionDesc.InternalError + "plugins dictionary not found ");
        }
    }

    /**
     * load config from flink file.
     */
    public FlinkConfiguration() throws IOException {
        String path = formatPath();
        flinkConfig = getFlinkConfigFromFile(path);

    }

    /**
     * get flinkcongfig
     * @return
     */
    public FlinkConfig getFlinkConfig() {
        return flinkConfig;
    }

    /**
     * parse properties
     * @param fileName
     * @return
     * @throws IOException
     */
    private  FlinkConfig getFlinkConfigFromFile(String fileName) throws IOException {
        Properties properties = new Properties();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        properties.load(bufferedReader);
        properties.forEach((key, value) -> configStorage.put((String) key,
                new JsonPrimitive((String) value)));
        FlinkConfig flinkConfig = new FlinkConfig();
        flinkConfig.setPort(Integer.valueOf(properties.getProperty(PORT)));
        flinkConfig.setAddress(properties.getProperty(ADDRESS));
        flinkConfig.setParallelism(Integer.valueOf(properties.getProperty(PARALLELISM)));
        flinkConfig.setSavepointDirectory(properties.getProperty(SAVEPOINT_DIRECTORY));
        flinkConfig.setJobManagerPort(Integer.valueOf(properties.getProperty(JOB_MANAGER_PORT)));
        return flinkConfig;
    }

}

