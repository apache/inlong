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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.inlong.manager.plugin.dto.FlinkConf;
import org.apache.inlong.manager.plugin.dto.JarFileInfo;
import org.apache.inlong.manager.plugin.dto.JarListInfo;
import org.apache.inlong.manager.plugin.dto.JarRunRequestbody;
import org.apache.inlong.manager.plugin.dto.Jars;
import org.apache.inlong.manager.plugin.dto.StopWithSavepointRequestBody;
import org.apache.inlong.manager.plugin.flink.enums.TaskCommitType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.inlong.manager.plugin.flink.FlinkUtils.translateFromFlinkConf;

@Slf4j
public class IntergrationTaskRunner implements Runnable {

    private static final Gson GSON = new GsonBuilder().serializeNulls().create();
    private FlinkService flinkService;
    private FlinkConf flinkConf;
    private Integer commitType;
    private static final Integer TRY_MAX_TIMES = 60;
    private static final Integer INTERVAL = 10;

    public IntergrationTaskRunner(FlinkService flinkService, FlinkConf flinkConf,Integer commitType) {
        this.flinkService = flinkService;
        this.flinkConf = flinkConf;
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
                    String jobId = flinkService.submitJobs(flinkConf);
                    flinkConf.setJobId(jobId);
                    log.info("Start job {} success in backend", jobId);
                } catch (Exception e) {
                    log.warn("Start job failed in backend");
                }
                break;
            case RESTART:
                try {
                    StopWithSavepointRequestBody stopWithSavepointRequestBody = new StopWithSavepointRequestBody();
                    stopWithSavepointRequestBody.setDrain(Constants.DRAIN);
                    stopWithSavepointRequestBody.setTargetDirectory(Constants.SAVEPOINT_DIRECTORY);
                    flinkService.stopJobs(flinkConf.getJobId(),stopWithSavepointRequestBody);
                    log.info("Stop job {} success in backend", flinkConf.getJobId());
                    int times = 0;
                    while (times < TRY_MAX_TIMES) {
                        JobStatus jobStatus = flinkService
                                .getJobStatus(flinkConf.getJobId());
                        if (jobStatus == FINISHED) {
                            String localJarPath = flinkConf.getLocalJarPath();
                            String jarResourceName = localJarPath.substring(localJarPath.lastIndexOf("/") + 1);

                            ResponseBody responseBody = flinkService.uploadJar(flinkConf.getLocalJarPath(),
                                    jarResourceName);
                            log.info("the jar file message is {}", responseBody.string());

                            ObjectMapper objectMapper = new ObjectMapper();
                            Jars jars = objectMapper.readValue(responseBody.string(), Jars.class);
                            String jarId = jars.getFilename().substring(jars.getFilename().lastIndexOf("/" + 1));
                            flinkConf.setResourceIds(jarId);

                            JarRunRequestbody jarRunRequestbody = translateFromFlinkConf(flinkConf);
                            flinkService.submitJobsWithoutJar(flinkConf.getResourceIds(), jarRunRequestbody);
                            log.info("Start job {} success in backend", flinkConf.getJobId());
                            break;
                        }
                        log.info("Try start job  but the job {} is {}", flinkConf.getJobId(), jobStatus.toString());
                        try {
                            Thread.sleep(INTERVAL * 1000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        times++;
                    }
                    log.info("Restart job {} success in backend", flinkConf.getJobId());
                } catch (Exception e) {
                    log.warn("Restart job {} failed in backend", flinkConf.getJobId());
                }
                break;
            case STOP:
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    StopWithSavepointRequestBody stopWithSavepointRequestBody = new StopWithSavepointRequestBody();
                    stopWithSavepointRequestBody.setDrain(Constants.DRAIN);
                    stopWithSavepointRequestBody.setTargetDirectory(Constants.SAVEPOINT_DIRECTORY);
                    ResponseBody body = flinkService.stopJobs(flinkConf.getJobId(), stopWithSavepointRequestBody);
                    Map<String, String> map =
                            objectMapper.readValue(body.string(), new TypeReference<HashMap<String, String>>() {
                            });
                    String triggerId = map.get("request-id");
                    if (StringUtils.isNotEmpty(triggerId)) {
                        ResponseBody responseBody = flinkService.triggerSavepoint(flinkConf.getJobId(),triggerId);
                        Map<String, JsonNode> dataflowMap =
                                objectMapper.convertValue(objectMapper.readTree(responseBody.string()),
                                new TypeReference<Map<String, JsonNode>>(){});
                        JsonNode jobStatus = dataflowMap.get("status");
                        JsonNode operation = dataflowMap.get("operation");
                        String status = jobStatus.get("id").asText();
                        String savepointPath = operation.get("location").asText();
                        flinkConf.setSavepointPath(savepointPath);
                        log.info("the jobId :{} status: {} ",flinkConf.getJobId(),status);
                    }
                } catch (Exception e) {
                    log.warn("Stop job {} failed in backend", flinkConf.getJobId());
                }
                break;
            case DELETE:
                try {
                    flinkService.deleteJobs(flinkConf.getJobId());
                    log.info("delete job {} success in backend", flinkConf.getJobId());
                    String resourceIds = flinkConf.getResourceIds();
                    String url = Constants.HTTP_URL + flinkConf.getEndpoint() + Constants.JARS_URL;
                    ResponseBody responseBody = flinkService.listJars(url);
                    ObjectMapper objectMapper = new ObjectMapper();
                    JarListInfo jarListInfo = objectMapper.readValue(responseBody.toString(),JarListInfo.class);
                    List<JarFileInfo> jarFileInfoList = jarListInfo.getFiles();
                    List<String> jarIdList = new ArrayList<>();
                    for (JarFileInfo jarFileInfo:jarFileInfoList) {
                        jarIdList.add(jarFileInfo.getId());
                    }
                    if (resourceIds != null && jarIdList.contains(resourceIds)) {
                        flinkService.deleteJars(resourceIds);
                    }
                    log.info("delete resource [{}] of job {} success in backend", flinkConf.getResourceIds(),
                            flinkConf.getJobId());
                } catch (Exception e) {
                    log.warn("delete job {} failed in backend,exception[{}]", flinkConf.getJobId(), e.getMessage());
                }
                break;
            default:
        }
    }
}
