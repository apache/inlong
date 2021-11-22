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

package org.apache.inlong.agent.core.job;

import static org.apache.inlong.agent.constants.AgentConstants.DEFAULT_JOB_DB_CACHE_CHECK_INTERVAL;
import static org.apache.inlong.agent.constants.AgentConstants.DEFAULT_JOB_DB_CACHE_TIME;
import static org.apache.inlong.agent.constants.AgentConstants.JOB_DB_CACHE_CHECK_INTERVAL;
import static org.apache.inlong.agent.constants.AgentConstants.JOB_DB_CACHE_TIME;
import static org.apache.inlong.agent.constants.JobConstants.JOB_ID;
import static org.apache.inlong.agent.constants.JobConstants.JOB_ID_PREFIX;
import static org.apache.inlong.agent.constants.JobConstants.JOB_INSTANCE_ID;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constants.AgentConstants;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.db.JobProfileDb;
import org.apache.inlong.agent.db.StateSearchKey;
import org.apache.inlong.agent.utils.AgentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobManager maintains lots of jobs, and communicate between server and task manager.
 */
public class JobManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

    // key is job instance id.
    private ConcurrentHashMap<String, JobWrapper> jobs;
    // jobs which are not accepted by running pool.
    private final ConcurrentHashMap<String, Job> pendingJobs;
    // job thread pool
    private final ThreadPoolExecutor runningPool;
    private final AgentManager agentManager;
    private final int monitorInterval;
    private final long jobDbCacheTime;
    private final long jobDbCacheCheckInterval;

    // job profile db is only used to recover instance which is not finished running.
    private final JobProfileDb jobConfDB;
    private final JobMetrics jobMetrics;
    private final AtomicLong index = new AtomicLong(0);

    /**
     * init job manager
     *
     * @param agentManager - agent manager
     */
    public JobManager(AgentManager agentManager, JobProfileDb jobConfDb) {
        this.jobConfDB = jobConfDb;
        this.agentManager = agentManager;
        // job thread pool for running
        this.runningPool = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("job"));
        this.jobs = new ConcurrentHashMap<>();
        this.pendingJobs = new ConcurrentHashMap<>();
        AgentConfiguration conf = AgentConfiguration.getAgentConf();
        this.monitorInterval = conf
                .getInt(
                    AgentConstants.JOB_MONITOR_INTERVAL, AgentConstants.DEFAULT_JOB_MONITOR_INTERVAL);
        this.jobDbCacheTime = conf.getLong(JOB_DB_CACHE_TIME, DEFAULT_JOB_DB_CACHE_TIME);
        this.jobDbCacheCheckInterval = conf.getLong(JOB_DB_CACHE_CHECK_INTERVAL, DEFAULT_JOB_DB_CACHE_CHECK_INTERVAL);
        this.jobMetrics = JobMetrics.create();
    }

    /**
     * submit job to work thread.
     *
     * @param job - job
     */
    public void addJob(Job job) {
        try {
            JobWrapper jobWrapper = new JobWrapper(agentManager, job);
            this.runningPool.execute(jobWrapper);
            JobWrapper jobWrapperRet = jobs.putIfAbsent(jobWrapper.getJob().getJobInstanceId(), jobWrapper);
            if (jobWrapperRet != null) {
                LOGGER.warn("{} has been added to running pool, "
                    + "cannot be added repeatedly", job.getJobInstanceId());
            } else {
                jobMetrics.runningJobs.incrementAndGet();
            }
        } catch (Exception rje) {
            LOGGER.debug("reject job {}", job.getJobInstanceId(), rje);
            pendingJobs.putIfAbsent(job.getJobInstanceId(), job);
        }
    }

    /**
     * add job profile
     * @param profile - job profile.
     */
    public boolean submitJobProfile(JobProfile profile) {
        if (profile == null || !profile.allRequiredKeyExist()) {
            LOGGER.error("profile is null or not all required key exists {}", profile == null ? null
                : profile.toJsonStr());
            return false;
        }
        String jobId = profile.get(JOB_ID);
        profile.set(JOB_INSTANCE_ID, AgentUtils.getUniqId(JOB_ID_PREFIX, jobId, index.incrementAndGet()));
        LOGGER.info("submit job profile {}", profile.toJsonStr());
        getJobConfDb().storeJobFirstTime(profile);
        addJob(new Job(profile));
        return true;
    }

    /**
     * delete job profile and stop job thread
     * @param jobInstancId
     */
    public void deleteJob(String jobInstancId) {
        JobWrapper jobWrapper = jobs.remove(jobInstancId);
        if (jobWrapper != null) {
            LOGGER.info("delete job instance with job id {}", jobInstancId);
            jobWrapper.cleanup();
            getJobConfDb().deleteJob(jobInstancId);
        }
    }

    /**
     * start all accepted jobs.
     */
    private void startJobs() {
        List<JobProfile> profileList = getJobConfDb().getAcceptedJobs();
        for (JobProfile profile : profileList) {
            LOGGER.info("init starting job from db {}", profile.toJsonStr());
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
     * check local db and delete old tasks.
     * @return
     */
    public Runnable dbStorageCheckThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    jobConfDB.removeExpireJobs(jobDbCacheTime);
                    TimeUnit.SECONDS.sleep(jobDbCacheCheckInterval);
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
            jobMetrics.runningJobs.decrementAndGet();
            LOGGER.info("job instance {} is success", jobId);
            // mark job as success.
            jobConfDB.updateJobState(jobId, StateSearchKey.SUCCESS);
        }
    }

    public void markJobAsFailed(String jobId) {
        JobWrapper wrapper = jobs.remove(jobId);
        if (wrapper != null) {
            LOGGER.info("job instance {} is failed", jobId);
            jobMetrics.runningJobs.decrementAndGet();
            jobMetrics.fatalJobs.incrementAndGet();
            // mark job as success.
            jobConfDB.updateJobState(jobId, StateSearchKey.FAILED);
        }
    }

    public JobProfileDb getJobConfDb() {
        return jobConfDB;
    }

    /**
     * check job existence using job file name
     * @return
     */
    public boolean checkJobExsit(String fileName) {
        return jobConfDB.getJob(fileName) != null;
    }

    public Map<String, JobWrapper> getJobs() {
        return jobs;
    }

    @Override
    public void start() {
        submitWorker(jobStateCheckThread());
        submitWorker(dbStorageCheckThread());
        startJobs();
    }

    @Override
    public void stop() throws Exception {
        waitForTerminate();
        this.runningPool.shutdown();
    }
}
