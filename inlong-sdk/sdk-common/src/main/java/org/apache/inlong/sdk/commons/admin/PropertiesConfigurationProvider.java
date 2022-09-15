/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.commons.admin;

import com.google.common.collect.Maps;

import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.node.AbstractConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * PropertiesConfigurationProvider
 */
public class PropertiesConfigurationProvider extends
        AbstractConfigurationProvider {

    public static final Logger LOG = LoggerFactory.getLogger(PropertiesConfigurationProvider.class);
    private static final String DEFAULT_PROPERTIES_IMPLEMENTATION = "java.util.Properties";

    private final Map<String, String> flumeConf;

    /**
     * PropertiesConfigurationProvider
     * 
     * @param rootName
     * @param flumeConf
     */
    public PropertiesConfigurationProvider(String rootName, Map<String, String> flumeConf) {
        super(rootName);
        this.flumeConf = flumeConf;
    }

    /**
     * PropertiesConfigurationProvider
     * 
     * @param rootName
     * @param flumeFile
     */
    public PropertiesConfigurationProvider(String rootName, File flumeFile) {
        super(rootName);
        this.flumeConf = getFlumeConf(flumeFile);
    }

    /**
     * getFlumeConfiguration
     * 
     * @return
     */
    @Override
    public FlumeConfiguration getFlumeConfiguration() {
        try {
            return new FlumeConfiguration(flumeConf);
        } catch (Exception e) {
            LOG.error("exception catch:" + e.getMessage(), e);
        }
        return new FlumeConfiguration(new HashMap<String, String>());
    }

    /**
     * getFlumeConf
     * @param file
     * @return
     */
    public static Map<String, String> getFlumeConf(File file) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String resolverClassName = System.getProperty("propertiesImplementation",
                    DEFAULT_PROPERTIES_IMPLEMENTATION);
            Class<? extends Properties> propsclass = Class.forName(resolverClassName)
                    .asSubclass(Properties.class);
            Properties properties = propsclass.newInstance();
            properties.load(reader);
            Map<String, String> result = Maps.newHashMap();
            Enumeration<?> propertyNames = properties.propertyNames();
            while (propertyNames.hasMoreElements()) {
                String name = (String) propertyNames.nextElement();
                String value = properties.getProperty(name);
                result.put(name, value);
            }
            return result;
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ex) {
                    LOG.warn(
                            "Unable to close file reader for file: " + file, ex);
                }
            }
        }
        return new HashMap<>();
    }
}
