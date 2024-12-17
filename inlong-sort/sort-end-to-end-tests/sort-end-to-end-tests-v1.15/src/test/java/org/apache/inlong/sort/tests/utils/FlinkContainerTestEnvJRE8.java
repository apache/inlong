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

package org.apache.inlong.sort.tests.utils;

import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

public abstract class FlinkContainerTestEnvJRE8 extends FlinkContainerTestEnv {

    @BeforeClass
    public static void before() {
        LOG.info("Starting containers...");
        jobManager =
                new GenericContainer<>("flink:1.15.4-scala_2.12-java8")
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_JM_ALIAS)
                        .withExposedPorts(JOB_MANAGER_REST_PORT, DEBUG_PORT)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withExposedPorts(JOB_MANAGER_REST_PORT)
                        .withLogConsumer(new Slf4jLogConsumer(JM_LOG));
        taskManager =
                new GenericContainer<>("flink:1.15.4-scala_2.12-java8")
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(INTER_CONTAINER_TM_ALIAS)
                        .withExposedPorts(DEBUG_PORT)
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .withEnv("OTEL_EXPORTER_ENDPOINT", "logcollector:4317")
                        .dependsOn(jobManager)
                        .withLogConsumer(new Slf4jLogConsumer(TM_LOG));

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        LOG.info("Containers are started.");
    }
}
