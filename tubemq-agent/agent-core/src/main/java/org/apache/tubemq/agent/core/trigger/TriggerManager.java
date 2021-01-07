/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.core.trigger;


import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.tubemq.agent.common.AbstractDaemon;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.apache.tubemq.agent.conf.JobProfile;
import org.apache.tubemq.agent.conf.TriggerProfile;
import org.apache.tubemq.agent.constants.AgentConstants;
import org.apache.tubemq.agent.constants.JobConstants;
import org.apache.tubemq.agent.core.AgentManager;
import org.apache.tubemq.agent.db.TriggerProfileDB;
import org.apache.tubemq.agent.plugin.Trigger;

/**
 * manager for triggers.
 */
public class TriggerManager extends AbstractDaemon {

    private final AgentManager manager;
    private final TriggerProfileDB triggerProfileDB;
    private final ConcurrentHashMap<String, Trigger> triggerMap;
    private final AgentConfiguration conf;
    private final int triggerFetchInterval;

    public TriggerManager(AgentManager manager, TriggerProfileDB triggerProfileDB) {
        this.conf = AgentConfiguration.getAgentConf();
        this.manager = manager;
        this.triggerProfileDB = triggerProfileDB;
        this.triggerMap = new ConcurrentHashMap<>();
        this.triggerFetchInterval = conf.getInt(AgentConstants.TRIGGER_FETCH_INTERVAL,
                AgentConstants.DEFAULT_TRIGGER_FETCH_INTERVAL);
    }

    /**
     * submit trigger profile.
     * @param triggerProfile - trigger profile
     */
    public void submitTrigger(TriggerProfile triggerProfile) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, IOException {
        // make sure all required key exists.
        if (triggerProfile.allRequiredKeyExist()) {
            triggerProfileDB.storeTrigger(triggerProfile);
            Class<?> triggerClass = Class.forName(triggerProfile.get(JobConstants.JOB_TRIGGER));
            Trigger trigger = (Trigger) triggerClass.newInstance();
            triggerMap.putIfAbsent(triggerProfile.get(JobConstants.JOB_ID), trigger);
            trigger.init(triggerProfile);
            trigger.run();
        }
    }

    private Runnable jobFetchThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    triggerMap.forEach((s, trigger) -> {
                        JobProfile profile = trigger.fetchJobProfile();
                        if (profile != null) {
                            manager.getJobManager().submitJobProfile(profile);
                        }
                    });
                    TimeUnit.SECONDS.sleep(triggerFetchInterval);
                } catch (Exception ignored) {}
            }

        };
    }

    /**
     * delete trigger by trigger id.
     * @param triggerId - trigger id.
     */
    public void deleteTrigger(String triggerId) {
        Trigger trigger = triggerMap.remove(triggerId);
        if (trigger != null) {
            trigger.destroy();
            // delete trigger from db
            triggerProfileDB.deleteTrigger(triggerId);
        }
    }

    /**
     * init all triggers when daemon started.
     */
    private void initTriggers() throws Exception {
        // fetch all triggers from db
        List<TriggerProfile> profileList = triggerProfileDB.getTriggers();
        for (TriggerProfile profile : profileList) {
            submitTrigger(profile);
        }
    }

    private void stopTriggers() {
        triggerMap.forEach((s, trigger) -> {
            trigger.destroy();
        });
    }

    @Override
    public void start() throws Exception {
        initTriggers();
        submitWorker(jobFetchThread());
    }

    @Override
    public void stop() {
        // stop all triggers
        stopTriggers();
    }
}
