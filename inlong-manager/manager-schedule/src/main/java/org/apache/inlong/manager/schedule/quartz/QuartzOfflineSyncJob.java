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

import org.apache.inlong.common.bounded.BoundaryType;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.pojo.schedule.OfflineJobRequest;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static org.apache.inlong.manager.schedule.util.ScheduleUtils.END_TIME;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.MANAGER_HOST;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.MANAGER_PORT;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.PASSWORD;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.USERNAME;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Service
public class QuartzOfflineSyncJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzOfflineSyncJob.class);

    private volatile InlongGroupClient groupClient;
    private long endTime;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOGGER.info("QuartzOfflineSyncJob run once");
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        initGroupClient(jobDataMap);

        String inlongGroupId = context.getJobDetail().getKey().getName();
        long lowerBoundary = context.getScheduledFireTime().getTime();
        long upperBoundary = context.getNextFireTime() == null ? endTime : context.getNextFireTime().getTime();

        OfflineJobRequest request = new OfflineJobRequest();
        request.setGroupId(inlongGroupId);
        request.setBoundaryType(BoundaryType.TIME.getType());
        request.setLowerBoundary(String.valueOf(lowerBoundary));
        request.setUpperBoundary(String.valueOf(upperBoundary));
        LOGGER.info("Starting submit offline job for group: {}, with lower boundary: {} and upper boundary: {}",
                inlongGroupId, lowerBoundary, upperBoundary);

        try {
            if (groupClient.submitOfflineJob(request)) {
                LOGGER.info("Successfully submitting offline job for group {}", inlongGroupId);
            } else {
                LOGGER.warn("Failed to submit offline job for group {}", inlongGroupId);
            }
        } catch (Exception e) {
            LOGGER.error("Exception to submit offline job for group {}, error msg: {}", inlongGroupId, e.getMessage());
        }

    }

    private void initGroupClient(JobDataMap jobDataMap) {
        if (groupClient == null) {
            String host = (String) jobDataMap.get(MANAGER_HOST);
            int port = (int) jobDataMap.get(MANAGER_PORT);
            String username = (String) jobDataMap.get(USERNAME);
            String password = (String) jobDataMap.get(PASSWORD);
            endTime = (long) jobDataMap.get(END_TIME);
            LOGGER.info("Initializing Inlong group client, with host: {}, port: {}, userName : {}, endTime: {}",
                    host, port, username, endTime);
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setAuthentication(new DefaultAuthentication(username, password));
            String serviceUrl = host + ":" + port;
            InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
            ClientFactory clientFactory = ClientUtils.getClientFactory(inlongClient.getConfiguration());
            groupClient = clientFactory.getGroupClient();
        }
    }
}
