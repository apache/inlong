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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * flink configuration. Only one instance in the process.
 * Basically it use properties file to store configurations.
 */
public class FlinkConfiguration extends AbstractConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkConfiguration.class);

    private static final String DEFAULT_CONFIG_FILE = "flink-sort-plugin.properties";

    private static final ArrayList<String> LOCAL_RESOURCES = new ArrayList<>();

    static {
        LOCAL_RESOURCES.add(DEFAULT_CONFIG_FILE);
    }

    private static volatile FlinkConfiguration flinkConfiguration = null;

    /**
     * load config from flink file.
     */
    private FlinkConfiguration() {
        for (String fileName : LOCAL_RESOURCES) {
            super.loadPropertiesResource(fileName);
        }
    }

    /**
     * singleton for flink configuration.
     * @return - static instance of flinkConfiguration
     */
    public static FlinkConfiguration getFlinkConf() {
        if (flinkConfiguration == null) {
            synchronized (FlinkConfiguration.class) {
                if (flinkConfiguration == null) {
                    flinkConfiguration = new FlinkConfiguration();
                }
            }
        }
        return flinkConfiguration;
    }

    private String getNextBackupFileName() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateStr = format.format(new Date(System.currentTimeMillis()));
        return DEFAULT_CONFIG_FILE + "." + dateStr;
    }

    @Override
    public boolean allRequiredKeyExist() {
        return true;
    }
}

