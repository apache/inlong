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

package org.apache.inlong.manager.schedule.airflow;

import org.apache.inlong.manager.schedule.exception.AirflowScheduleException;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

@Slf4j
public class AirflowContainerEnv {

    public static String BASE_URL = "http://localhost:8080";
    public static String AIRFLOW_USERNAME = "airflow";
    public static String AIRFLOW_PASSWORD = "airflow";
    public static String NORMAL_POSTFIX = "_normal";
    public static String CORN_POSTFIX = "_cron";
    public static String AIRFLOW_SCHEDULER_CONTAINER_NAME = "airflow-scheduler_1";
    public static String DOCKER_COMPOSE_YAML_PATH = "src/test/resources/airflow/docker-compose.yaml";
    public static String DEFAULT_DAGS_PATH = "/opt/airflow/dags/";

    private static DockerComposeContainer<?> environment;
    private static OkHttpClient httpClient = new OkHttpClient();

    public static void setUp() {
        // Step 1: Start only the airflow-init service
        environment = new DockerComposeContainer<>(new File(DOCKER_COMPOSE_YAML_PATH))
                .withServices("airflow-init")
                .withEnv("AIRFLOW_UID", "$(id -u)");
        // Start the environment
        environment.start();
        // Step 2: Wait until the "airflow-init" service has completed initialization
        // Once initialized, stop the init-only environment and start the full environment
        environment.stop();
        // Step 3: Start all services in detached mode after initialization
        environment = new DockerComposeContainer<>(new File(DOCKER_COMPOSE_YAML_PATH))
                .withEnv("AIRFLOW_UID", "0")
                .withEnv("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
                .withEnv("AIRFLOW__API__AUTH_BACKEND",
                        "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth");
        environment.start();
        copyTestDAGs();
        waitForDAGsLoad("dag_cleaner");
        log.info("Airflow runtime environment created successfully.");
    }

    private static void copyTestDAGs() {
        // After the DAG file is created, the scheduler will regularly scan the DAG file directory and
        // then load it into memory for scheduling. In order to quickly test the update and unregister, two
        // test DAGs need to be loaded at the beginning.
        Optional<ContainerState> container = environment.getContainerByServiceName(AIRFLOW_SCHEDULER_CONTAINER_NAME);
        if (container.isPresent()) {
            ContainerState airflowScheduler = container.get();
            Path dagPath1 = Paths.get("src/test/resources/airflow/dag_cleaner.py").toAbsolutePath();
            Path dagPath2 = Paths.get("src/test/resources/airflow/dag_creator.py").toAbsolutePath();
            Path dagPath3 = Paths.get("src/test/resources/airflow/testGroup_cron.py").toAbsolutePath();
            Path dagPath4 = Paths.get("src/test/resources/airflow/testGroup_normal.py").toAbsolutePath();
            airflowScheduler.copyFileToContainer(MountableFile.forHostPath(dagPath1),
                    DEFAULT_DAGS_PATH.concat("dag_cleaner.py"));
            airflowScheduler.copyFileToContainer(MountableFile.forHostPath(dagPath2),
                    DEFAULT_DAGS_PATH.concat("dag_creator.py"));
            airflowScheduler.copyFileToContainer(MountableFile.forHostPath(dagPath3),
                    DEFAULT_DAGS_PATH.concat("testGroup_cron.py"));
            airflowScheduler.copyFileToContainer(MountableFile.forHostPath(dagPath4),
                    DEFAULT_DAGS_PATH.concat("testGroup_normal.py"));
            try {
                String result =
                        airflowScheduler.execInContainer("bash", "-c", "ls ".concat(DEFAULT_DAGS_PATH)).getStdout();
                log.info(DEFAULT_DAGS_PATH.concat(" has file: {}"), result);
            } catch (Exception e) {
                log.warn(String.format(
                        "Copying the test DAG file may have failed. Docker Container command(\"%s\") execution failed.",
                        "ls ".contains(DEFAULT_DAGS_PATH)), e);
            }
        } else {
            log.error(String.format("Copying test DAG file failed. Airflow scheduler container(%s) does not exist.",
                    AIRFLOW_SCHEDULER_CONTAINER_NAME));
            throw new AirflowScheduleException("Copying test DAG file failed.");
        }
        log.info("Copy test DAG file successfully.");
    }

    public static void waitForDAGsLoad(String dagId) {
        int total = 10;
        // Waiting for Airflow to load the initial DAG
        while (total > 0) {
            String credential = okhttp3.Credentials.basic(AIRFLOW_USERNAME, AIRFLOW_PASSWORD);
            Request request = new Request.Builder()
                    .url(BASE_URL + "/api/v1/dags/" + dagId + "/details")
                    .header("Authorization", credential)
                    .build();
            try (Response response = httpClient.newCall(request).execute()) {
                if (response.code() == 200) {
                    break;
                }
            } catch (Exception e) {
                log.error("The request to check if the original DAG exists failed: {}", e.getMessage(), e);
            }
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            total--;
        }
        log.info("DAG successfully loaded.");
    }
}
