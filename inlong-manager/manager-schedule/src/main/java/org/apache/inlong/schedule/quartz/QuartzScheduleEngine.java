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

package org.apache.inlong.schedule.quartz;

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.schedule.ScheduleEngine;
import org.apache.inlong.schedule.exception.QuartzScheduleException;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.inlong.schedule.util.ScheduleUtils.genQuartzJobDetail;
import static org.apache.inlong.schedule.util.ScheduleUtils.genQuartzTrigger;

/**
 * The default implementation of schedule engine based on Quartz scheduler. Response for processing
 * the register/unregister/update requests from {@link QuartzScheduleClient}
 * */
@Getter
public class QuartzScheduleEngine implements ScheduleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzScheduleEngine.class);

    private final Scheduler scheduler;
    private final Set<String> scheduledJobSet = new HashSet<>();

    public QuartzScheduleEngine() {
        try {
            this.scheduler = new StdSchedulerFactory().getScheduler();
            LOGGER.info("Quartz scheduler engine initialized");
        } catch (SchedulerException e) {
            throw new QuartzScheduleException("Failed to init quartz scheduler ", e);
        }
    }

    @Override
    public void start() {
        try {
            // add listener
            scheduler.getListenerManager().addSchedulerListener(new QuartzSchedulerListener(this));
            scheduler.start();
            LOGGER.info("Quartz scheduler engine started");
        } catch (SchedulerException e) {
            throw new QuartzScheduleException("Failed to start quartz scheduler ", e);
        }
    }

    /**
     * Clean job info from scheduledJobSet after trigger finalized.
     * */
    public boolean triggerFinalized(Trigger trigger) {
        String jobName = trigger.getJobKey().getName();
        LOGGER.info("Trigger finalized for job {}", jobName);
        return scheduledJobSet.remove(jobName);
    }

    /**
     * Handle schedule register.
     * @param scheduleInfo schedule info to register
     * */
    @Override
    public boolean handleRegister(ScheduleInfo scheduleInfo) {
        return handleRegister(scheduleInfo, QuartzOfflineSyncJob.class);
    }

    @VisibleForTesting
    public boolean handleRegister(ScheduleInfo scheduleInfo, Class<? extends QuartzOfflineSyncJob> clz) {
        if (scheduledJobSet.contains(scheduleInfo.getInlongGroupId())) {
            throw new QuartzScheduleException("Group " + scheduleInfo.getInlongGroupId() + " is already registered");
        }
        JobDetail jobDetail = genQuartzJobDetail(scheduleInfo, clz);
        Trigger trigger = genQuartzTrigger(jobDetail, scheduleInfo);
        try {
            scheduler.scheduleJob(jobDetail, trigger);
            scheduledJobSet.add(scheduleInfo.getInlongGroupId());
            LOGGER.info("Registered new schedule info for {}", scheduleInfo.getInlongGroupId());
        } catch (SchedulerException e) {
            throw new QuartzScheduleException(e.getMessage());
        }
        return false;
    }

    /**
     * Handle schedule unregister.
     * @param scheduleInfo schedule info to unregister
     * */
    @Override
    public boolean handleUnregister(ScheduleInfo scheduleInfo) {
        if (scheduledJobSet.contains(scheduleInfo.getInlongGroupId())) {
            try {
                scheduler.deleteJob(new JobKey(scheduleInfo.getInlongGroupId()));
            } catch (SchedulerException e) {
                throw new QuartzScheduleException(e.getMessage());
            }
        }
        scheduledJobSet.remove(scheduleInfo.getInlongGroupId());
        LOGGER.info("Un-registered schedule info for {}", scheduleInfo.getInlongGroupId());
        return true;
    }

    /**
     * Handle schedule update.
     * @param scheduleInfo schedule info to update
     * */
    @Override
    public boolean handleUpdate(ScheduleInfo scheduleInfo) {
        return handleUpdate(scheduleInfo, QuartzOfflineSyncJob.class);
    }

    @VisibleForTesting
    public boolean handleUpdate(ScheduleInfo scheduleInfo, Class<? extends QuartzOfflineSyncJob> clz) {
        handleUnregister(scheduleInfo);
        handleRegister(scheduleInfo, clz);
        LOGGER.info("Updated schedule info for {}", scheduleInfo.getInlongGroupId());
        return false;
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
                LOGGER.info("Quartz scheduler engine stopped");
            } catch (SchedulerException e) {
                throw new QuartzScheduleException("Failed to stop quartz scheduler ", e);
            }
        }
    }

}
