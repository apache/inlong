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
package org.apache.tubemq.agent.core;


import org.apache.tubemq.agent.common.AbstractDaemon;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.apache.tubemq.agent.conf.ProfileFetcher;
import org.apache.tubemq.agent.constants.AgentConstants;
import org.apache.tubemq.agent.core.job.JobManager;
import org.apache.tubemq.agent.core.task.TaskManager;
import org.apache.tubemq.agent.core.trigger.TriggerManager;
import org.apache.tubemq.agent.db.DB;
import org.apache.tubemq.agent.db.JobProfileDB;
import org.apache.tubemq.agent.db.TriggerProfileDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agent Manager, the bridge for job manager, task manager, db e.t.c it manages agent level
 * operations and communicates with outside system.
 */
public class AgentManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentManager.class);
    private static JobManager jobManager;
    private static TaskManager taskManager;
    private static TriggerManager triggerManager;



    private final long waitTime;
    private final ProfileFetcher fetcher;
    private final AgentConfiguration conf;
    private final DB db;

    public AgentManager() {
        conf = AgentConfiguration.getAgentConf();
        this.db = initDB();
        fetcher = initFetcher();
        triggerManager = new TriggerManager(this, new TriggerProfileDB(db));
        jobManager = new JobManager(this, new JobProfileDB(db));
        taskManager = new TaskManager(this);

        this.waitTime = conf.getLong(
            AgentConstants.THREAD_POOL_AWAIT_TIME, AgentConstants.DEFAULT_THREAD_POOL_AWAIT_TIME);
    }

    /**
     * init fetch by class name
     *
     * @return
     */
    private ProfileFetcher initFetcher() {
        try {
            return (ProfileFetcher)
                Class.forName(conf.get(AgentConstants.AGENT_FETCHER_CLASSNAME))
                    .newInstance();
        } catch (Exception ex) {
            LOGGER.warn("cannot find fetcher, ignore it {}", ex.getMessage());
        }
        return null;
    }

    /**
     * init db by class name
     *
     * @return db
     */
    private DB initDB() {
        try {
            // db is a required component, so if not init correctly,
            // throw exception and stop running.
            return (DB) Class.forName(conf.get(
                AgentConstants.AGENT_DB_CLASSNAME, AgentConstants.DEFAULT_AGENT_DB_CLASSNAME))
                .newInstance();
        } catch (Exception ex) {
            throw new UnsupportedClassVersionError(ex.getMessage());
        }
    }

    public JobManager getJobManager() {
        return jobManager;
    }

    public TaskManager getTaskManager() {
        return taskManager;
    }

    @Override
    public void join() {
        super.join();
        jobManager.join();
        taskManager.join();
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("starting agent manager");
        triggerManager.start();
        jobManager.start();
        taskManager.start();
        if (fetcher != null) {
            fetcher.start();
        }
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
        LOGGER.info("stopping agent manager");
        // close in order: trigger -> job -> task
        triggerManager.stop();
        jobManager.stop();
        taskManager.stop();
        this.db.close();
    }
}
