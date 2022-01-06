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

package org.apache.inlong.agent.plugin;

import static org.apache.inlong.agent.constants.AgentConstants.AGENT_FETCH_CENTER_INTERVAL_SECONDS;

import java.util.concurrent.LinkedBlockingQueue;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.core.AgentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiniAgent.class);
    private final AgentManager manager;
    private final LinkedBlockingQueue<JobProfile> queueJobs;

    public MiniAgent() {
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        conf.setInt(AGENT_FETCH_CENTER_INTERVAL_SECONDS, 1);
        manager = new AgentManager();
        queueJobs = new LinkedBlockingQueue<>(100);

    }

    public void start() throws Exception {
        manager.start();
    }

    public void submitJob(JobProfile profile) {
        manager.getJobManager().submitFileJobProfile(profile);
    }

    public void submitTriggerJob(JobProfile profile) {
        TriggerProfile triggerProfile = TriggerProfile.parseJobProfile(profile);
        manager.getTriggerManager().addTrigger(triggerProfile);
    }

    public AgentManager getManager() {
        return manager;
    }

    public void stop() throws Exception {
        manager.stop();
    }
}
