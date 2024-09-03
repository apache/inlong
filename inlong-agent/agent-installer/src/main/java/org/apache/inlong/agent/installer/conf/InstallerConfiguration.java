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

package org.apache.inlong.agent.installer.conf;

import org.apache.inlong.agent.conf.AbstractConfiguration;

import java.util.ArrayList;

/**
 * Installer configuration. Only one instance in the process.
 * Basically it use properties file to save configurations.
 */
public class InstallerConfiguration extends AbstractConfiguration {

    public static final String DEFAULT_CONFIG_FILE = "installer.properties";
    private static final ArrayList<String> LOCAL_RESOURCES = new ArrayList<>();
    private static volatile InstallerConfiguration installerConf = null;

    static {
        LOCAL_RESOURCES.add(DEFAULT_CONFIG_FILE);
    }

    /**
     * Load config from file.
     */
    private InstallerConfiguration() {
        for (String fileName : LOCAL_RESOURCES) {
            super.loadPropertiesResource(fileName);
        }
    }

    /**
     * Singleton for agent configuration.
     *
     * @return static instance of InstallerConfiguration
     */
    public static InstallerConfiguration getInstallerConf() {
        if (installerConf == null) {
            synchronized (InstallerConfiguration.class) {
                if (installerConf == null) {
                    installerConf = new InstallerConfiguration();
                }
            }
        }
        return installerConf;
    }

    @Override
    public boolean allRequiredKeyExist() {
        return true;
    }
}
