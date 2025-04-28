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

import org.apache.inlong.common.bounded.BoundaryType;
import org.apache.inlong.common.util.ConcurrentHashSet;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.airflow.AirflowConnection;
import org.apache.inlong.manager.pojo.schedule.airflow.DAG;
import org.apache.inlong.manager.pojo.schedule.airflow.DAGCollection;
import org.apache.inlong.manager.pojo.schedule.airflow.DAGRun;
import org.apache.inlong.manager.pojo.schedule.airflow.DAGRunConf;
import org.apache.inlong.manager.schedule.ScheduleEngine;
import org.apache.inlong.manager.schedule.ScheduleUnit;
import org.apache.inlong.manager.schedule.airflow.api.AirflowResponse;
import org.apache.inlong.manager.schedule.airflow.api.connection.AirflowConnectionCreator;
import org.apache.inlong.manager.schedule.airflow.api.connection.AirflowConnectionGetter;
import org.apache.inlong.manager.schedule.airflow.api.dag.DAGCollectionUpdater;
import org.apache.inlong.manager.schedule.airflow.api.dag.DAGDeletor;
import org.apache.inlong.manager.schedule.airflow.api.dag.DAGUpdater;
import org.apache.inlong.manager.schedule.airflow.api.dagruns.DAGRunsTrigger;
import org.apache.inlong.manager.schedule.airflow.config.AirflowConfig;
import org.apache.inlong.manager.schedule.airflow.util.DAGUtil;
import org.apache.inlong.manager.schedule.airflow.util.DateUtil;
import org.apache.inlong.manager.schedule.exception.AirflowScheduleException;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.inlong.manager.schedule.airflow.AirFlowAPIConstant.DEFAULT_TIMEZONE;
import static org.apache.inlong.manager.schedule.airflow.AirFlowAPIConstant.INLONG_OFFLINE_DAG_TASK_PREFIX;
import static org.apache.inlong.manager.schedule.airflow.AirFlowAPIConstant.SUBMIT_OFFLINE_JOB_URI;
import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.DAG_DUPLICATE;
import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.INIT_CONNECTION_FAILED;
import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.SCHEDULE_ENGINE_SHUTDOWN_FAILED;
import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.SCHEDULE_TASK_REGISTER_FAILED;
import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.SCHEDULE_TASK_UPDATE_FAILED;
import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.TASK_DAG_SWITCH_FAILED;

/**
 * Response for processing the start/register/unregister/update/stop requests from {@link AirflowScheduleClient}
 */
@Service
public class AirflowScheduleEngine implements ScheduleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(AirflowScheduleEngine.class);
    private final Set<String> scheduledJobSet = new ConcurrentHashSet<>();
    private AirflowServerClient serverClient;
    private AirflowConfig airflowConfig;

    public AirflowScheduleEngine(AirflowServerClient serverClient, AirflowConfig airflowConfig) {
        this.serverClient = serverClient;
        this.airflowConfig = airflowConfig;
        start();
    }

    @Override
    public void start() {
        try {
            // Create authentication information for the Inlong Manger API used by AirFlow
            initConnection();
            // Check if DagCleaner and DagCreator exist and unpause them
            switchOriginalDAG(false);
            // Start all task DAGs and load all DAG ID(Group Id) into the local cache
            switchAllTaskDAG(false);
            LOGGER.info("Airflow initialization succeeded.");
        } catch (Exception e) {
            LOGGER.error("Airflow initialization failed.", e);
        }
    }

    private void initConnection() throws Exception {
        LOGGER.info("Initializing Inlong Manager AirflowConnection for Airflow ... ");
        // Check if Airflow has the Inlong AirflowConnection
        AirflowResponse<AirflowConnection> response = serverClient.sendRequest(
                new AirflowConnectionGetter(airflowConfig.getConnectionId()));
        if (!response.isSuccess()) {
            AirflowConnection newConn = new AirflowConnection(airflowConfig.getConnectionId(), "HTTP", "",
                    airflowConfig.getInlongManagerHost(), airflowConfig.getInlongUsername(), SUBMIT_OFFLINE_JOB_URI,
                    airflowConfig.getInlongManagerPort(), airflowConfig.getInlongPassword(), "");
            response = serverClient.sendRequest(new AirflowConnectionCreator(newConn));
            LOGGER.info("AirflowConnection registration response: {}", response.toString());
            if (!response.isSuccess()) {
                LOGGER.error("Initialization connection failed.");
                throw new AirflowScheduleException(INIT_CONNECTION_FAILED, "Initialization connection failed.");
            }
        }
    }

    private void switchOriginalDAG(boolean isPaused) {
        for (String dagId : Arrays.asList(airflowConfig.getDagCleanerId(), airflowConfig.getDagCreatorId())) {
            try {
                AirflowResponse<DAG> response = serverClient.sendRequest(new DAGUpdater(dagId, isPaused));
                LOGGER.info("Response to {} the original DAG : {}", isPaused ? "stop" : "start", response.toString());
                if (!response.isSuccess()) {
                    throw new AirflowScheduleException(TASK_DAG_SWITCH_FAILED,
                            String.format("%s does not exist or failed to %s.", dagId, (isPaused ? "stop" : "start")));
                }
            } catch (Exception e) {
                LOGGER.error("The original DAG {} failed.", isPaused ? "stop" : "start", e);
                throw new AirflowScheduleException(TASK_DAG_SWITCH_FAILED,
                        String.format("The original DAG %s failed: %s.", isPaused ? "stop" : "start", e.getMessage()));
            }
        }
    }

    private void switchAllTaskDAG(boolean isPaused) {
        try {
            AirflowResponse<DAGCollection> response = serverClient.sendRequest(
                    new DAGCollectionUpdater(INLONG_OFFLINE_DAG_TASK_PREFIX, isPaused));
            LOGGER.info("Response to {} task DAG : {}", isPaused ? "stop" : "start", response.toString());
            if (!response.isSuccess()) {
                throw new AirflowScheduleException(TASK_DAG_SWITCH_FAILED,
                        String.format("Failed to %s task DAGs.", isPaused ? "stop" : "start"));
            }
            if (!isPaused) {
                List<DAG> dagList = response.getData().getDags();
                if (dagList != null) {
                    dagList.forEach(dag -> scheduledJobSet
                            .add(dag.getDagId().substring(INLONG_OFFLINE_DAG_TASK_PREFIX.length() - 1)));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to {} task DAGs.", isPaused ? "stop" : "start", e);
            throw new AirflowScheduleException(TASK_DAG_SWITCH_FAILED,
                    String.format("Failed to %s task DAGs: %s", isPaused ? "stop" : "start", e.getMessage()));
        }
    }

    @Override
    public boolean handleRegister(ScheduleInfo scheduleInfo) {
        try {
            LOGGER.info("Registering DAG for {}", scheduleInfo.getInlongGroupId());
            return doRegister(scheduleInfo, true);
        } catch (Exception e) {
            LOGGER.error("The Airflow scheduling task with Group ID {} failed to register.",
                    scheduleInfo.getInlongGroupId(), e);
            throw new AirflowScheduleException(SCHEDULE_TASK_REGISTER_FAILED,
                    String.format("The Airflow scheduling task with Group ID %s failed to register: %s",
                            scheduleInfo.getInlongGroupId(), e.getMessage()));
        }
    }

    @Override
    public boolean handleUnregister(String groupId) {
        LOGGER.info("Unregistering Airflow Dag with GroupId {} ", groupId);
        if (scheduledJobSet.contains(groupId)) {
            try {
                if (!completelyDelete(DAGUtil.buildDAGIdByGroupId(groupId))) {
                    return false;
                }
            } catch (Exception e) {
                LOGGER.warn("May not be completely removed {}", groupId, e);
            }
        }
        scheduledJobSet.remove(groupId);
        LOGGER.info("Un-registered airflow schedule info for {}", groupId);
        return true;
    }

    private boolean completelyDelete(String groupId) throws Exception {
        // Trigger the removal of the DAG file for the Cleaner DAG
        DAGRunConf dagRunConf = DAGRunConf.builder()
                .inlongGroupId(DAGUtil.buildDAGIdByGroupId(groupId)).build();
        AirflowResponse<DAGRun> response = serverClient.sendRequest(
                new DAGRunsTrigger(airflowConfig.getDagCleanerId(), ImmutableMap.of("conf", dagRunConf)));
        LOGGER.info("Response to DAG file clearing: {}", response.toString());
        if (!response.isSuccess()) {
            LOGGER.warn("Failed to delete DAG file corresponding to {}.", groupId);
            return false;
        }
        // Delete DAG tasks that have been loaded into memory
        AirflowResponse<Object> deleteResponse = serverClient.sendRequest(new DAGDeletor(groupId));
        LOGGER.info("Response to DAG scheduling instance clearing: {}", deleteResponse.toString());
        if (!deleteResponse.isSuccess()) {
            LOGGER.warn("Failed to delete DAG instance corresponding to {}.", groupId);
        }
        return deleteResponse.isSuccess();
    }

    @Override
    public boolean handleUpdate(ScheduleInfo scheduleInfo) {
        try {
            LOGGER.info("Updating DAG for {}", scheduleInfo.getInlongGroupId());
            return doRegister(scheduleInfo, false);
        } catch (Exception e) {
            LOGGER.error("The Airflow scheduling task with Group ID {} failed to update.",
                    scheduleInfo.getInlongGroupId(), e);
            throw new AirflowScheduleException(SCHEDULE_TASK_UPDATE_FAILED,
                    String.format("The Airflow scheduling task with Group ID %s failed to update: %s",
                            scheduleInfo.getInlongGroupId(), e.getMessage()));
        }
    }

    public boolean doRegister(ScheduleInfo scheduleInfo, boolean isFirst) throws Exception {
        if (isFirst && scheduledJobSet.contains(scheduleInfo.getInlongGroupId())) {
            throw new AirflowScheduleException(DAG_DUPLICATE,
                    String.format("Group %s is already registered", scheduleInfo.getInlongGroupId()));
        }
        DAGRunConf.DAGRunConfBuilder confBuilder = DAGRunConf.builder()
                .inlongGroupId(scheduleInfo.getInlongGroupId())
                .startTime(scheduleInfo.getStartTime().getTime())
                .endTime(scheduleInfo.getEndTime().getTime())
                .boundaryType(BoundaryType.TIME.getType())
                .connectionId(airflowConfig.getConnectionId())
                .timezone(DEFAULT_TIMEZONE);
        if (scheduleInfo.getScheduleType() == 1) {
            confBuilder = confBuilder.cronExpr(scheduleInfo.getCrontabExpression());
        } else {
            confBuilder = confBuilder.secondsInterval(DateUtil.intervalToSeconds(scheduleInfo.getScheduleInterval(),
                    scheduleInfo.getScheduleUnit()))
                    .startTime(ScheduleUnit.getScheduleUnit(scheduleInfo.getScheduleUnit()) == ScheduleUnit.ONE_ROUND
                            ? scheduleInfo.getEndTime().getTime()
                            : scheduleInfo.getStartTime().getTime());
        }
        DAGRunConf dagRunConf = confBuilder.build();
        AirflowResponse<DAGRun> response = serverClient.sendRequest(
                new DAGRunsTrigger(airflowConfig.getDagCreatorId(), ImmutableMap.of("conf", dagRunConf)));
        LOGGER.info("DAG {} response: {}", isFirst ? "registration" : "update", response.toString());
        if (response.isSuccess()) {
            scheduledJobSet.add(scheduleInfo.getInlongGroupId());
        }
        return response.isSuccess();
    }

    @Override
    public void stop() {
        try {
            switchOriginalDAG(true);
            switchAllTaskDAG(true);
        } catch (Exception e) {
            LOGGER.error("Airflow Schedule Engine shutdown failed: ", e);
            throw new AirflowScheduleException(SCHEDULE_ENGINE_SHUTDOWN_FAILED,
                    String.format("Airflow Schedule Engine shutdown failed: %s", e.getMessage()));
        }
    }
}
