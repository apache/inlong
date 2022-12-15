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

package org.apache.inlong.agent.core.trigger;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.FileCollectType;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.core.AgentManager;
import org.apache.inlong.agent.core.job.JobWrapper;
import org.apache.inlong.agent.core.task.Task;
import org.apache.inlong.agent.db.JobProfileDb;
import org.apache.inlong.agent.db.StateSearchKey;
import org.apache.inlong.agent.db.TriggerProfileDb;
import org.apache.inlong.agent.plugin.Trigger;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.inlong.agent.constant.AgentConstants.DEFAULT_TRIGGER_MAX_RUNNING_NUM;
import static org.apache.inlong.agent.constant.AgentConstants.TRIGGER_MAX_RUNNING_NUM;
import static org.apache.inlong.agent.constant.JobConstants.JOB_DIR_FILTER_PATTERNS;
import static org.apache.inlong.agent.constant.JobConstants.JOB_ID;
import static org.apache.inlong.agent.constant.JobConstants.TRIGGER_ONLY_ONE_JOB;

/**
 * manager for triggers.
 */
public class TriggerManager extends AbstractDaemon {

    public static final int JOB_CHECK_INTERVAL = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerManager.class);
    private final AgentManager manager;
    private final TriggerProfileDb triggerProfileDB;
    private final ConcurrentHashMap<String, Trigger> triggerMap;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, JobProfile>> triggerJobMap;
    private final AgentConfiguration conf;
    private final int triggerFetchInterval;
    private final int maxRunningNum;

    public TriggerManager(AgentManager manager, TriggerProfileDb triggerProfileDb) {
        this.conf = AgentConfiguration.getAgentConf();
        this.manager = manager;
        this.triggerProfileDB = triggerProfileDb;
        this.triggerMap = new ConcurrentHashMap<>();
        this.triggerJobMap = new ConcurrentHashMap<>();
        this.triggerFetchInterval = conf.getInt(AgentConstants.TRIGGER_FETCH_INTERVAL,
                AgentConstants.DEFAULT_TRIGGER_FETCH_INTERVAL);
        this.maxRunningNum = conf.getInt(TRIGGER_MAX_RUNNING_NUM, DEFAULT_TRIGGER_MAX_RUNNING_NUM);
    }

    /**
     * Restore trigger task.
     *
     * @param triggerProfile trigger profile
     */
    public boolean restoreTrigger(TriggerProfile triggerProfile) {
        try {
            Class<?> triggerClass = Class.forName(triggerProfile.get(JobConstants.JOB_TRIGGER));
            Trigger trigger = (Trigger) triggerClass.newInstance();
            String triggerId = triggerProfile.get(JOB_ID);
            if (triggerMap.containsKey(triggerId)) {
                deleteTrigger(triggerId);
                LOGGER.warn("trigger {} is running, stop it", triggerId);
            }
            triggerMap.put(triggerId, trigger);
            trigger.init(triggerProfile);
            trigger.run();
        } catch (Throwable ex) {
            LOGGER.error("add trigger error: ", ex);
            ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
            return false;
        }
        return true;
    }

    public Trigger getTrigger(String triggerId) {
        return triggerMap.get(triggerId);
    }

    /**
     * Submit trigger task.It guarantees eventual consistency.
     * 1.store db kv.
     * 2.start trigger thread.
     *
     * @param triggerProfile trigger profile
     * @return true if success
     */
    public boolean submitTrigger(TriggerProfile triggerProfile) {
        // make sure all required key exists.
        if (!triggerProfile.allRequiredKeyExist() || this.triggerMap.size() > maxRunningNum) {
            LOGGER.error("trigger {} not all required key exists or size {} exceed {}",
                    triggerProfile.toJsonStr(), this.triggerMap.size(), maxRunningNum);
            return false;
        }
        // repeat check
        if (triggerProfileDB.getTriggers().stream()
                .anyMatch(profile -> profile.getTriggerId().equals(triggerProfile.getTriggerId()))) {
            return true;
        }

        LOGGER.info("submit trigger {}", triggerProfile);
        triggerProfileDB.storeTrigger(triggerProfile);
        preprocessTrigger(triggerProfile);
        restoreTrigger(triggerProfile);
        return true;
    }

    /**
     * Submit trigger task.It guarantees eventual consistency.
     * 1.stop trigger task and related collecting job.
     * 2.delete db kv.
     *
     * @param triggerId trigger profile.
     */
    public boolean deleteTrigger(String triggerId) {
        // repeat check
        if (!triggerProfileDB.getTriggers().stream()
                .anyMatch(profile -> profile.getTriggerId().equals(triggerId))) {
            return true;
        }

        LOGGER.info("delete trigger {}", triggerId);
        Trigger trigger = triggerMap.remove(triggerId);
        if (trigger != null) {
            deleteRelatedJobs(triggerId);
            trigger.destroy();
        }
        triggerProfileDB.deleteTrigger(triggerId);
        return true;
    }

    /**
     * Preprocessing before adding trigger, default value FULL
     *
     * FULL: All directory by regex
     * INCREMENT: Directory entry created
     */
    private void preprocessTrigger(TriggerProfile profile) {
        String syncType = profile.get(JobConstants.JOB_FILE_COLLECT_TYPE, FileCollectType.FULL);
        if (FileCollectType.INCREMENT.equals(syncType)) {
            return;
        }
        LOGGER.info("initialize submit full sync trigger {}", profile.getTriggerId());
        manager.getJobManager().submitFileJobProfile(profile);
    }

    private Runnable jobFetchThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    triggerMap.forEach((s, trigger) -> {
                        JobProfile profile = trigger.fetchJobProfile();
                        if (profile != null) {
                            TriggerProfile triggerProfile = trigger.getTriggerProfile();
                            if (triggerProfile.getBoolean(TRIGGER_ONLY_ONE_JOB, false)) {
                                deleteRelatedJobs(triggerProfile.getTriggerId());
                            }
                            Map<String, JobWrapper> jobWrapperMap = manager.getJobManager().getJobs();
                            // running job then add new task
                            if (isRunningJob(profile, jobWrapperMap)) {
                                jobWrapperMap.get(profile.getInstanceId()).addTask(profile);
                            }
                            // not running job then add job
                            if (isExistJob(profile)) {
                                manager.getJobManager().submitFileJobProfile(profile);
                                addToTriggerMap(profile.get(JOB_ID), profile);
                            }
                        }
                    });
                    TimeUnit.SECONDS.sleep(triggerFetchInterval);
                } catch (Throwable e) {
                    LOGGER.info("ignored exception: ", e);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
                }
            }
        };
    }

    private boolean isRunningJob(JobProfile profile, Map<String, JobWrapper> jobWrapperMap) {
        try {
            if (jobWrapperMap == null || jobWrapperMap.get(profile.getInstanceId()) == null) {
                return false;
            }

            JobWrapper jobWrapper = jobWrapperMap.get(profile.getInstanceId());
            List<Task> tasks = jobWrapper.getAllTasks();
            if (tasks == null) {
                return true;
            }
            for (Task task : tasks) {
                if (task.getJobConf().hasKey(profile.get(JOB_DIR_FILTER_PATTERNS))) {
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            LOGGER.warn("not found job {} in the jobs, error: ", profile.toJsonStr(), e);
            return false;
        }
    }

    private boolean isExistJob(JobProfile profile) {
        List<JobProfile> jobProfileList = getJobProfiles();
        AtomicBoolean isExist = new AtomicBoolean(false);
        jobProfileList.forEach(job -> {
            if (profile.get(JOB_ID).equals(job.get(JOB_ID))) {
                isExist.set(true);
            }
        });
        return !isExist.get();
    }

    /**
     * delete jobs generated by the trigger
     */
    private void deleteRelatedJobs(String triggerId) {
        LOGGER.info("start to delete related jobs in triggerId {}", triggerId);
        List<JobProfile> jobProfileList = getJobProfiles();
        jobProfileList.forEach(jobProfile -> {
            if (Objects.equals(jobProfile.get(JOB_ID), triggerId)) {
                deleteJob(jobProfile.getInstanceId());
            }
        });
        triggerJobMap.remove(triggerId);
    }

    private List<JobProfile> getJobProfiles() {
        JobProfileDb jobProfileDb = manager.getJobProfileDb();
        List<JobProfile> jobProfileList = jobProfileDb.getJobsByState(StateSearchKey.RUNNING);
        jobProfileList.addAll(jobProfileDb.getJobsByState(StateSearchKey.ACCEPTED));
        return jobProfileList;
    }

    private void deleteJob(String jobInstanceId) {
        manager.getJobManager().deleteJob(jobInstanceId);
    }

    private Runnable jobCheckThread() {
        return () -> {
            while (isRunnable()) {
                try {
                    triggerJobMap.forEach((s, jobProfiles) -> {
                        for (String jobId : jobProfiles.keySet()) {
                            Map<String, JobWrapper> jobs = manager.getJobManager().getJobs();
                            if (jobs.get(jobId) == null) {
                                triggerJobMap.remove(jobId);
                            }
                        }
                    });
                    TimeUnit.MINUTES.sleep(JOB_CHECK_INTERVAL);
                } catch (Throwable e) {
                    LOGGER.info("ignored Exception ", e);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
                }
            }

        };
    }

    /**
     * need to put profile in triggerJobMap
     */
    private void addToTriggerMap(String triggerId, JobProfile profile) {
        ConcurrentHashMap<String, JobProfile> tmpList = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, JobProfile> jobWrappers = triggerJobMap.putIfAbsent(triggerId, tmpList);
        if (jobWrappers == null) {
            jobWrappers = tmpList;
        }
        jobWrappers.putIfAbsent(profile.getInstanceId(), profile);
    }

    /**
     * init all triggers when daemon started.
     */
    private void initTriggers() {
        // fetch all triggers from db
        List<TriggerProfile> profileList = triggerProfileDB.getTriggers();
        for (TriggerProfile profile : profileList) {
            restoreTrigger(profile);
        }
    }

    private void stopTriggers() {
        triggerMap.forEach((s, trigger) -> {
            trigger.destroy();
        });
    }

    @Override
    public void start() {
        initTriggers();
        submitWorker(jobFetchThread());
        submitWorker(jobCheckThread());
    }

    @Override
    public void stop() {
        // stop all triggers
        stopTriggers();
    }

}
