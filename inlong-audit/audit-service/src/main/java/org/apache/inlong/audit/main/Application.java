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

package org.apache.inlong.audit.main;

import org.apache.inlong.audit.service.ApiService;
import org.apache.inlong.audit.service.EtlService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);
    private static final EtlService etlService = new EtlService();
    private static final ApiService apiService = new ApiService();

    public static void main(String[] args) {
        try {
            etlService.start();
            apiService.start();

            stopIfKilled();
        } catch (Exception ex) {
            LOGGER.error("Running exception: ", ex);
        }
    }

    private static void stopIfKilled() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                etlService.stop();
                apiService.stop();
                LOGGER.info("Stopping gracefully");
            } catch (Exception ex) {
                LOGGER.error("Stop error: ", ex);
            }
        }));
    }
}
