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

package org.apache.inlong.agent.installer;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.ProfileFetcher;
import org.apache.inlong.agent.installer.conf.InstallerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Installer Manager, the bridge for job manager, task manager, store e.t.c it manages agent level operations and
 * communicates with outside system.
 */
public class Manager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(Manager.class);
    private final ModuleManager moduleManager;
    private final ProfileFetcher fetcher;
    private final InstallerConfiguration conf;

    public Manager() {
        conf = InstallerConfiguration.getInstallerConf();
        moduleManager = new ModuleManager();
        fetcher = initFetcher(this);
    }

    /**
     * Init fetch by class name
     */
    private ProfileFetcher initFetcher(Manager manager) {
        try {
            return new ManagerFetcher(manager);
        } catch (Exception ex) {
            LOGGER.warn("cannot find fetcher: ", ex);
        }
        return null;
    }

    public ProfileFetcher getFetcher() {
        return fetcher;
    }

    public ModuleManager getModuleManager() {
        return moduleManager;
    }

    @Override
    public void join() {
        super.join();
        moduleManager.join();
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Start installer manager");
        moduleManager.start();
        LOGGER.info("Start installer manager end");
        LOGGER.info("Start fetcher");
        if (fetcher != null) {
            fetcher.start();
        }
        LOGGER.info("Start fetcher end");
    }

    /**
     * It should guarantee thread-safe, and can be invoked many times.
     *
     * @throws Exception exceptions
     */
    @Override
    public void stop() throws Exception {
        if (fetcher != null) {
            fetcher.stop();
        }
        // TODO: change job state which is in running state.
        LOGGER.info("Stopping installer manager");
        moduleManager.stop();
    }
}
