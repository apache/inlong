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

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.inner.client.ClientFactory;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;

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

import static org.apache.inlong.manager.schedule.util.ScheduleUtils.MANAGER_HOST;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.MANAGER_PORT;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.SECRETE_ID;
import static org.apache.inlong.manager.schedule.util.ScheduleUtils.SECRETE_KEY;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Service
public class QuartzOfflineSyncJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzOfflineSyncJob.class);

    private volatile InlongGroupClient groupClient;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        LOGGER.info("QuartzOfflineSyncJob run once");
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        initGroupClientIfNeeded(jobDataMap);
        String inlongGroupId = context.getJobDetail().getKey().getName();
        LOGGER.info("Starting submit offline job for group {}", inlongGroupId);
        if (groupClient.submitOfflineJob(inlongGroupId)) {
            LOGGER.info("Successfully submitting offline job for group {}", inlongGroupId);
        } else {
            LOGGER.warn("Failed to submit offline job for group {}", inlongGroupId);
        }
    }

    private void initGroupClientIfNeeded(JobDataMap jobDataMap) {
        if (groupClient == null) {
            String host = (String) jobDataMap.get(MANAGER_HOST);
            int port = (int) jobDataMap.get(MANAGER_PORT);
            String secreteId = (String) jobDataMap.get(SECRETE_ID);
            String secreteKey = (String) jobDataMap.get(SECRETE_KEY);
            LOGGER.info("Initializing Inlong group client, with host: {}, port: {}, userName : {}",
                    host, port, secreteId);
            ClientConfiguration configuration = new ClientConfiguration();
            configuration.setAuthentication(new DefaultAuthentication(secreteId, secreteKey));
            String serviceUrl = host + ":" + port;
            InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
            ClientFactory clientFactory = ClientUtils.getClientFactory(inlongClient.getConfiguration());
            groupClient = clientFactory.getGroupClient();
        }
    }
}
