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

package org.apache.inlong.manager.plugin.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.inlong.manager.plugin.flink.dto.FlinkConfig;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.plugin.flink.dto.StopWithSavepointRequestBody;
import org.apache.inlong.manager.plugin.flink.enums.TaskCommitType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.inlong.manager.plugin.util.FlinkUtils.getExceptionStackMsg;

@Slf4j
public class IntergrationTaskRunner implements Runnable {

    private FlinkService flinkService;
    private FlinkInfo flinkInfo;
    private Integer commitType;
    private static final Integer TRY_MAX_TIMES = 60;
    private static final Integer INTERVAL = 10;

    public IntergrationTaskRunner(FlinkService flinkService, FlinkInfo flinkInfo,Integer commitType) {
        this.flinkService = flinkService;
        this.flinkInfo = flinkInfo;
        this.commitType = commitType;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        TaskCommitType commitType = TaskCommitType.getInstance(this.commitType);
        if (commitType == null) {
            commitType = TaskCommitType.START_NOW;
        }
        switch (commitType) {
            case START_NOW:
                try {
                    String jobId = flinkService.submitJobs(flinkInfo);
                    flinkInfo.setJobId(jobId);
                    log.info("Start job {} success in backend", jobId);
                } catch (Exception e) {
                    String msg = String.format("Start job %s failed in backend exception[%s]", flinkInfo.getJobId(),
                            getExceptionStackMsg(e));
                    log.warn(msg);
                    flinkInfo.setException(true);
                    flinkInfo.setExceptionMsg(msg);
                }
                break;
            case RESUME:
                try {
                    String jobId = flinkService.restore(flinkInfo);
                    log.info("Restore job {} success in backend", jobId);
                } catch (Exception e) {
                    String msg = String.format("Restore job %s failed in backend exception[%s]", flinkInfo.getJobId(),
                            getExceptionStackMsg(e));
                    log.warn(msg);
                    flinkInfo.setException(true);
                    flinkInfo.setExceptionMsg(msg);
                }
                break;
            case RESTART:
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    StopWithSavepointRequestBody stopWithSavepointRequestBody = new StopWithSavepointRequestBody();
                    stopWithSavepointRequestBody.setDrain(Constants.DRAIN);
                    stopWithSavepointRequestBody.setTargetDirectory(Constants.SAVEPOINT_DIRECTORY);
                    String requestBody = objectMapper.writeValueAsString(stopWithSavepointRequestBody);
                    ResponseBody body = flinkService.stopJobs(flinkInfo.getJobId(), requestBody);
                    Map<String, String> map =
                            objectMapper.readValue(body.string(), new TypeReference<HashMap<String, String>>() {
                            });
                    String triggerId = map.get("request-id");
                    if (StringUtils.isNotEmpty(triggerId)) {
                        ResponseBody responseBody = flinkService.triggerSavepoint(flinkInfo.getJobId(), triggerId);
                        Map<String, JsonNode> dataflowMap =
                                objectMapper.convertValue(objectMapper.readTree(responseBody.string()),
                                        new TypeReference<Map<String, JsonNode>>() {
                                        });
                        JsonNode jobStopStatus = dataflowMap.get("status");
                        JsonNode operation = dataflowMap.get("operation");
                        String status = jobStopStatus.get("id").asText();
                        String savepointPath = operation.get("location").asText();
                        flinkInfo.setSavepointPath(savepointPath);
                        log.info("the jobId :{} status: {} ", flinkInfo.getJobId(), status);
                    }
                    int times = 0;
                    while (times < TRY_MAX_TIMES) {
                        JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
                        // restore job
                        if (jobStatus == FINISHED) {
                            try {
                                String jobId = flinkService.restore(flinkInfo);
                                log.info("Restore job {} success in backend", jobId);
                            } catch (Exception e) {
                                log.warn("Restore job failed in backend");
                            }
                            break;
                        }
                        log.info("Try start job  but the job {} is {}", flinkInfo.getJobId(), jobStatus.toString());
                        try {
                            Thread.sleep(INTERVAL * 1000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        times++;
                    }
                    log.info("Restart job {} success in backend", flinkInfo.getJobId());
                } catch (Exception e) {
                    String msg = String.format("Restart job %s failed in backend exception[%s]", flinkInfo.getJobId(),
                            getExceptionStackMsg(e));
                    log.warn(msg);
                    flinkInfo.setException(true);
                    flinkInfo.setExceptionMsg(msg);
                }
                break;
            case STOP:
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    StopWithSavepointRequestBody stopWithSavepointRequestBody = new StopWithSavepointRequestBody();
                    stopWithSavepointRequestBody.setDrain(Constants.DRAIN);
                    FlinkConfig flinkConfig = flinkService.getFlinkConfig();
                    stopWithSavepointRequestBody.setTargetDirectory(flinkConfig.getSavepointDirectory());
                    String requestBody = objectMapper.writeValueAsString(stopWithSavepointRequestBody);
                    ResponseBody body = flinkService.stopJobs(flinkInfo.getJobId(), requestBody);
                    Map<String, String> map =
                            objectMapper.readValue(body.string(), new TypeReference<HashMap<String, String>>() {
                            });
                    String triggerId = map.get("request-id");
                    if (StringUtils.isNotEmpty(triggerId)) {
                        ResponseBody responseBody = flinkService.triggerSavepoint(flinkInfo.getJobId(),triggerId);
                        Map<String, JsonNode> dataflowMap =
                                objectMapper.convertValue(objectMapper.readTree(responseBody.string()),
                                new TypeReference<Map<String, JsonNode>>(){});
                        JsonNode jobStatus = dataflowMap.get("status");
                        JsonNode operation = dataflowMap.get("operation");
                        String status = jobStatus.get("id").asText();
                        String savepointPath = operation.get("location").asText();
                        flinkInfo.setSavepointPath(savepointPath);
                        log.info("the jobId {} status: {} ", flinkInfo.getJobId(),status);
                    }
                    JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
                    if (jobStatus.isTerminalState()) {
                        log.info("stop job {}, status: {}, success in backend", flinkInfo.getJobId(), jobStatus);
                    } else {
                        log.info("stop job {}, status: {}, fail in backend", flinkInfo.getJobId(),jobStatus);
                    }
                } catch (Exception e) {
                    String msg = String.format("stop job %s failed in backend exception[%s]", flinkInfo.getJobId(),
                            getExceptionStackMsg(e));
                    log.warn(msg);
                    flinkInfo.setException(true);
                    flinkInfo.setExceptionMsg(msg);
                }
                break;
            case DELETE:
                try {
                    flinkService.deleteJobs(flinkInfo.getJobId());
                    log.info("delete job {} success in backend", flinkInfo.getJobId());
                    JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
                    if (jobStatus.isTerminalState()) {
                        log.info("delete  job {} success in backend", flinkInfo.getJobId());
                    } else {
                        log.info("delete  job {} failed in backend", flinkInfo.getJobId());
                    }
                } catch (Exception e) {
                    String msg = String.format("delete job %s failed in backend exception[%s]", flinkInfo.getJobId(),
                            getExceptionStackMsg(e));
                    log.warn(msg);
                    flinkInfo.setException(true);
                    flinkInfo.setExceptionMsg(msg);
                }
                break;
            default:
                String msg = "not found commitType";
                flinkInfo.setException(true);
                log.warn(msg);
                flinkInfo.setExceptionMsg(msg);
        }
    }
}
