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

package org.apache.inlong.manager.schedule.quartz;

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.schedule.ScheduleEngine;
import org.apache.inlong.manager.schedule.exception.QuartzScheduleException;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

import static org.apache.inlong.manager.schedule.util.ScheduleUtils.genQuartzJobDetail;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.genQuartzTrigger;

/**
 * The default implementation of schedule engine based on Quartz scheduler. Response for processing
 * the register/unregister/update requests from {@link QuartzScheduleClient}
 * */
@Getter
@Service
public class QuartzScheduleEngine implements ScheduleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzScheduleEngine.class);

    @Value("${server.host:127.0.0.1}")
    private String host;

    @Value("${server.port:8083}")
    private int port;

    @Value("${default.admin.user:admin}")
    private String username;

    @Value("${default.admin.password:inlong}")
    private String password;

    private final Scheduler scheduler;
    private final Set<String> scheduledJobSet = new HashSet<>();

    public QuartzScheduleEngine() {
        try {
            this.scheduler = new StdSchedulerFactory().getScheduler();
            LOGGER.info("Quartz scheduler engine initialized");
        } catch (SchedulerException e) {
            throw new QuartzScheduleException("Failed to init quartz scheduler ", e);
        }
        start();
    }

    @Override
    public void start() {
        try {
            // add listener
            scheduler.getListenerManager().addSchedulerListener(new QuartzSchedulerListener(this));
            scheduler.start();
            LOGGER.info("Quartz scheduler engine started, inlong manager host {}, port {}, secretId {}",
                    host, port, username);
        } catch (SchedulerException e) {
            throw new QuartzScheduleException("Failed to start quartz scheduler ", e);
        }
    }

    /**
     * Clean job info from scheduledJobSet after trigger finalized.
     * */
    public boolean triggerFinalized(Trigger trigger) {
        String jobName = trigger.getJobKey().getName();
        LOGGER.info("Quartz trigger finalized for job {}", jobName);
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
    public boolean handleRegister(ScheduleInfo scheduleInfo, Class<? extends Job> clz) {
        if (scheduledJobSet.contains(scheduleInfo.getInlongGroupId())) {
            throw new QuartzScheduleException("Group " + scheduleInfo.getInlongGroupId() + " is already registered");
        }
        JobDetail jobDetail = genQuartzJobDetail(scheduleInfo, clz, host, port, username, password);
        Trigger trigger = genQuartzTrigger(jobDetail, scheduleInfo);
        try {
            scheduler.scheduleJob(jobDetail, trigger);
            scheduledJobSet.add(scheduleInfo.getInlongGroupId());
            LOGGER.info("Registered new quartz schedule info for {}", scheduleInfo.getInlongGroupId());
        } catch (SchedulerException e) {
            throw new QuartzScheduleException(e.getMessage());
        }
        return false;
    }

    /**
     * Handle schedule unregister.
     * @param groupId group to un-register schedule info
     * */
    @Override
    public boolean handleUnregister(String groupId) {
        if (scheduledJobSet.contains(groupId)) {
            try {
                scheduler.deleteJob(new JobKey(groupId));
            } catch (SchedulerException e) {
                throw new QuartzScheduleException(e.getMessage());
            }
        }
        scheduledJobSet.remove(groupId);
        LOGGER.info("Un-registered quartz schedule info for {}", groupId);
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
    public boolean handleUpdate(ScheduleInfo scheduleInfo, Class<? extends Job> clz) {
        handleUnregister(scheduleInfo.getInlongGroupId());
        handleRegister(scheduleInfo, clz);
        LOGGER.info("Updated quartz schedule info for {}", scheduleInfo.getInlongGroupId());
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
