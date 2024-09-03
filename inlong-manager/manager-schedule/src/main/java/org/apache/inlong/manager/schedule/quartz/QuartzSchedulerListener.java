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

import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.SchedulerListener;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation for quartz scheduler listener.
 * */
public class QuartzSchedulerListener implements SchedulerListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzSchedulerListener.class);

    private QuartzScheduleEngine quartzScheduleEngine;

    public QuartzSchedulerListener(QuartzScheduleEngine quartzScheduleEngine) {
        this.quartzScheduleEngine = quartzScheduleEngine;
    }

    @Override
    public void jobScheduled(Trigger trigger) {
        LOGGER.info("Quartz job with key {} scheduled", trigger.getKey().getName());
    }

    @Override
    public void jobUnscheduled(TriggerKey triggerKey) {
        LOGGER.info("Quartz job with key {} un-scheduled", triggerKey.getName());
    }

    @Override
    public void triggerFinalized(Trigger trigger) {
        quartzScheduleEngine.triggerFinalized(trigger);
        LOGGER.info("Quartz trigger with key {} startTime {} ande endTime {} is finalized",
                trigger.getKey().getName(), trigger.getStartTime(), trigger.getEndTime());
    }

    @Override
    public void triggerPaused(TriggerKey triggerKey) {
        LOGGER.info("Quartz trigger with key {} paused", triggerKey.getName());
    }

    @Override
    public void triggersPaused(String triggerGroup) {

    }

    @Override
    public void triggerResumed(TriggerKey triggerKey) {
        LOGGER.info("Quartz trigger with key {} Resume", triggerKey.getName());
    }

    @Override
    public void triggersResumed(String triggerGroup) {

    }

    @Override
    public void jobAdded(JobDetail jobDetail) {
        LOGGER.info("New quartz job added, name {}", jobDetail.getKey().getName());
    }

    @Override
    public void jobDeleted(JobKey jobKey) {
        LOGGER.info("Quartz job deleted, name {}", jobKey.getName());
    }

    @Override
    public void jobPaused(JobKey jobKey) {
        LOGGER.info("Quartz job paused, name {}", jobKey.getName());
    }

    @Override
    public void jobsPaused(String jobGroup) {

    }

    @Override
    public void jobResumed(JobKey jobKey) {
        LOGGER.info("Quartz job resumed, name {}", jobKey.getName());
    }

    @Override
    public void jobsResumed(String jobGroup) {

    }

    @Override
    public void schedulerError(String msg, SchedulerException cause) {
        LOGGER.warn("Quartz schedule exception, errorMsg {}", msg, cause);
    }

    @Override
    public void schedulerInStandbyMode() {

    }

    @Override
    public void schedulerStarted() {
        LOGGER.warn("Quartz scheduler started");
    }

    @Override
    public void schedulerStarting() {
        LOGGER.warn("Quartz scheduler starting");
    }

    @Override
    public void schedulerShutdown() {
        LOGGER.warn("Quartz scheduler shutdown");
    }

    @Override
    public void schedulerShuttingdown() {
        LOGGER.warn("Quartz scheduler shutting down");
    }

    @Override
    public void schedulingDataCleared() {
        LOGGER.warn("Quartz scheduler data cleared");
    }
}
