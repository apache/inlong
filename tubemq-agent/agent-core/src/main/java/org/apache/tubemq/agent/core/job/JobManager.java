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
package org.apache.tubemq.agent.core.job;


import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.tubemq.agent.common.AbstractDaemon;
import org.apache.tubemq.agent.common.AgentThreadFactory;
import org.apache.tubemq.agent.conf.AgentConfiguration;
import org.apache.tubemq.agent.conf.JobProfile;
import org.apache.tubemq.agent.constants.AgentConstants;
import org.apache.tubemq.agent.core.AgentManager;
import org.apache.tubemq.agent.db.JobProfileDB;
import org.apache.tubemq.agent.db.StateSearchKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobManager maintains lots of jobs, and communicate between server and task manager.
 */
public class JobManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

    // key is job instance id.
    private final ConcurrentHashMap<String, JobWrapper> jobs;
    // jobs which are not accepted by running pool.
    private final ConcurrentHashMap<String, Job> pendingJobs;
    // job thread pool
    private final ThreadPoolExecutor runningPool;
    private final AgentManager agentManager;
    private final int monitorInterval;

    private final JobProfileDB jobConfDB;

    /**
     * init job manager
     *
     * @param agentManager - agent manager
     */
    public JobManager(AgentManager agentManager, JobProfileDB jobConfDB) {
        this.jobConfDB = jobConfDB;
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        this.agentManager = agentManager;
        // job thread pool for running
        this.runningPool = new ThreadPoolExecutor(
                conf.getInt(AgentConstants.JOB_RUNNING_THREAD_CORE_SIZE,
                        AgentConstants.DEFAULT_JOB_RUNNING_THREAD_CORE_SIZE),
                conf.getInt(
                    AgentConstants.JOB_RUNNING_THREAD_MAX_SIZE, AgentConstants.DEFAULT_JOB_RUNNING_THREAD_MAX_SIZE),
                conf.getLong(AgentConstants.JOB_RUNNING_THREAD_KEEP_ALIVE,
                        AgentConstants.DEFAULT_JOB_RUNNING_THREAD_KEEP_ALIVE),
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(
                        conf.getInt(
                            AgentConstants.JOB_PENDING_MAX, AgentConstants.DEFAULT_JOB_PENDING_MAX)),
                new AgentThreadFactory("job"));
        this.jobs = new ConcurrentHashMap<>();
        this.pendingJobs = new ConcurrentHashMap<>();
        this.monitorInterval = conf
                .getInt(
                    AgentConstants.JOB_MONITOR_INTERVAL, AgentConstants.DEFAULT_JOB_MONITOR_INTERVAL);

    }

    /**
     * submit job to work thread.
     *
     * @param job - job
     * @return - whether submitting job successfully.
     */
    private void addJob(Job job) {
        try {
            JobWrapper jobWrapper = new JobWrapper(agentManager, job);
            this.runningPool.execute(jobWrapper);
            jobs.putIfAbsent(jobWrapper.getJob().getJobId(), jobWrapper);
        } catch (Exception rje) {
            LOGGER.error("reject job {}", job.getJobId(), rje);
            pendingJobs.putIfAbsent(job.getJobId(), job);
        }
    }

    /**
     * add job profile
     * @param profile - job profile.
     */
    public void submitJobProfile(JobProfile profile) {
        if (profile != null && profile.allRequiredKeyExist()) {
            getJobConfDB().storeJob(profile);
            addJob(new Job(profile));
        }
    }

    /**
     * start all accepted jobs.
     */
    private void startJobs() {
        List<JobProfile> profileList = getJobConfDB().getAcceptedJobs();
        for (JobProfile profile : profileList) {
            addJob(new Job(profile));
        }
    }

    public Runnable jobStateCheckThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    // check pending jobs and try to submit again.
                    for (String jobId : pendingJobs.keySet()) {
                        Job job = pendingJobs.remove(jobId);
                        if (job != null) {
                            addJob(job);
                        }
                    }
                    TimeUnit.SECONDS.sleep(monitorInterval);
                } catch (Exception ex) {
                    LOGGER.error("error caught", ex);
                }
            }
        };
    }

    /**
     * mark job as success by job id.
     * @param jobId - job id
     */
    public void markJobAsSuccess(String jobId) {
        JobWrapper wrapper = jobs.remove(jobId);
        if (wrapper != null) {
            LOGGER.info("job instance {} is success", jobId);
            // mark job as success.
            jobConfDB.updateJobState(jobId, StateSearchKey.SUCCESS);
        }
    }

    public JobProfileDB getJobConfDB() {
        return jobConfDB;
    }

    @Override
    public void start() {
        submitWorker(jobStateCheckThread());
        startJobs();
    }

    @Override
    public void stop() throws Exception {
        waitForTerminate();
        this.runningPool.shutdown();
    }
}
