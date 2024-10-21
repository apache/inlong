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

package org.apache.inlong.audit.service.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Configuration. Only one instance in the process.
 * Basically it use properties file to store configurations.
 */
public class Configuration {

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);
    public static final String DEFAULT_CONFIG_FILE = "conf/audit-service.properties";

    private static volatile Configuration conf = null;
    Properties properties = new Properties();

    /**
     * load config from agent file.
     */
    private Configuration() {
        try (FileInputStream fileInputStream = new FileInputStream(DEFAULT_CONFIG_FILE)) {
            properties.load(fileInputStream);
        } catch (Exception e) {
            LOGGER.error("Configuration has exception!", e);
        }
    }

    /**
     * singleton for configuration.
     *
     * @return static instance of Configuration
     */
    public static Configuration getInstance() {
        if (conf == null) {
            synchronized (Configuration.class) {
                if (conf == null) {
                    conf = new Configuration();
                }
            }
        }
        return conf;
    }

    /**
     * @param key
     * @param defaultValue
     * @return
     */
    public String get(String key, String defaultValue) {
        Object value = properties.get(key);
        return value == null ? defaultValue : value.toString();
    }

    public boolean get(String key, boolean defaultValue) {
        Object value = properties.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean((String) value);
    }

    /**
     * @param key
     * @param defaultValue
     * @return
     */
    public int get(String key, int defaultValue) {
        Object value = properties.get(key);
        return value == null ? defaultValue : Integer.parseInt((String) value);
    }

    /**
     * Get double value
     *
     * @param key
     * @param defaultValue
     * @return
     */
    public double get(String key, double defaultValue) {
        Object value = properties.get(key);
        return value == null ? defaultValue : Double.parseDouble((String) value);
    }

    /**
     * @param key
     * @return
     */
    public String get(String key) {
        Object value = properties.get(key);
        return value == null ? null : value.toString();
    }
}
