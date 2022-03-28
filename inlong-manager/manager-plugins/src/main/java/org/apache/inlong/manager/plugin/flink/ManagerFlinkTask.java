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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.plugin.flink.enums.BusinessExceptionDesc;
import org.apache.inlong.manager.plugin.flink.enums.TaskCommitType;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.JobStatus.CANCELED;
import static org.apache.flink.api.common.JobStatus.FAILED;
import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.INITIALIZING;
import static org.apache.flink.api.common.JobStatus.RUNNING;

/**
 * flink operation
 */
@Slf4j
public class ManagerFlinkTask {

    private FlinkService flinkService;

    public ManagerFlinkTask(FlinkService flinkService) {
        this.flinkService = flinkService;
    }

    public void start(FlinkInfo flinkInfo) throws IOException {
        String jobId = flinkInfo.getJobId();
        //Start a new task without savepoint
        if (StringUtils.isEmpty(jobId)) {
            try {
                Future<?> future = TaskRunService.submit(
                        new IntergrationTaskRunner(flinkService, flinkInfo,
                                TaskCommitType.START_NOW.getCode()));
                future.get();
            } catch (Exception e) {
                log.warn("Flink job some exception [{}]", e.getMessage());
                throw new BusinessException(BusinessExceptionDesc.UnsupportedOperation
                        + e.getMessage());
            }
            //Restore an old task with savepoint
            } else {
            JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkInfo.getJobId());
            if (jobDetailsInfo == null) {
                throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                        + String.format("Flink job %s not found", flinkInfo.getJobId()));
            }
            JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
           if (jobStatus == FINISHED && StringUtils.isNotEmpty(flinkInfo.getSavepointPath())) {
               try {
                   Future<?> future = TaskRunService.submit(
                           new IntergrationTaskRunner(flinkService, flinkInfo,
                                   TaskCommitType.RESUME.getCode()));
                   future.get();
               } catch (Exception e) {
                   log.warn("Flink job some exception [{}]", e.getMessage());
                   throw new BusinessException(BusinessExceptionDesc.UnsupportedOperation
                           + e.getMessage());
               }
           }
        }
    }

    /**
     * @param flinkInfo
     * @param dataflow
     */
    public void genPath(FlinkInfo flinkInfo, String dataflow) {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        path = path.substring(0, path.lastIndexOf(File.separator));
//        String resource = "resource";
//        String jarPath = path + File.separator + resource + File.separator + Constants.SORT_JAR;
//        File file = new File(jarPath);
//        if (!file.exists()) {
//            log.warn("file path:[{}] not found sort jar", jarPath);
//            throw new BusinessException(BusinessExceptionDesc.InternalError + " not found sort jar");
//        }
//        flinkInfo.setLocalJarPath(jarPath);
        if (path.contains("inlong-manager")) {
            path = path.substring(0, path.indexOf("inlong-manager"));
            String resource = "inlong-sort";
            String jarPath = path  + resource + File.separator + Constants.SORT_JAR;
            File file = new File(jarPath);
            if (!file.exists()) {
                log.warn("file path:[{}] not found sort jar", jarPath);
                throw new BusinessException(BusinessExceptionDesc.InternalError + " not found sort jar");
            }
            flinkInfo.setLocalJarPath(jarPath);
        } else {
            throw new BusinessException(BusinessExceptionDesc.InternalError + " inlong-manager dic not found");
        }
        if (FlinkUtils.writeConfigToFile(path, flinkInfo.getJobName(), dataflow)) {
            flinkInfo.setLocalConfPath(path + File.separator + flinkInfo.getJobName());
        } else {
            throw new BusinessException(BusinessExceptionDesc.InternalError + " write file fail");
        }
    }

    /**
     * restart flinkjob
     * @param flinkInfo
     * @throws Exception
     * @throws IOException
     */
    public void restart(FlinkInfo flinkInfo) throws Exception, IOException {
        JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkInfo.getJobId());
        if (jobDetailsInfo == null) {
            throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                    + String.format("Flink job %s not found", flinkInfo.getJobId()));
        }
        JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
        if (jobStatus == RUNNING) {
            Future<?> future = TaskRunService.submit(
                    new IntergrationTaskRunner(flinkService, flinkInfo,
                            TaskCommitType.RESTART.getCode()));
            future.get();
        } else {
            throw new BusinessException(BusinessExceptionDesc.FailedOperation.getMessage()
                    + String.format("Flink job %s restart fail", flinkInfo.getJobId()));
        }
    }

    /**
     * stop flinkjob
     * @param flinkInfo
     * @throws Exception
     */
    public void stop(FlinkInfo flinkInfo) throws Exception {
        JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkInfo.getJobId());
        if (jobDetailsInfo == null) {
            throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                    + String.format("Flink job %s not found", flinkInfo.getJobId()));
        }
        JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
        if (jobStatus == RUNNING) {
            Future<?> future = TaskRunService.submit(
                    new IntergrationTaskRunner(flinkService, flinkInfo,
                            TaskCommitType.STOP.getCode()));
            future.get();
        } else {
            throw new BusinessException(BusinessExceptionDesc.FailedOperation.getMessage()
                    + String.format("Flink job %s pause fail", flinkInfo.getJobId()));
        }
    }

    /**
     * delete flinkjob
     * @param flinkInfo
     * @throws Exception
     */
    public void delete(FlinkInfo flinkInfo) throws Exception {
        JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkInfo.getJobId());
        if (jobDetailsInfo == null) {
            throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                    + String.format("Flink job %s not found", flinkInfo.getJobId()));
        }
        JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
        switch (jobStatus) {
            case CANCELED:
                throw new BusinessException(BusinessExceptionDesc.UnsupportedOperation
                        + "not support delete when task has been canceled");
            case RUNNING:
                Future<?> future = TaskRunService.submit(
                        new IntergrationTaskRunner(flinkService, flinkInfo,
                                TaskCommitType.DELETE.getCode()));
                future.get();
                break;
            default:
                throw new BusinessException(BusinessExceptionDesc.UnsupportedOperation
                        + "not support delete when task is OPERATING");
        }
    }

    /**
     * poll flink status
     * @param flinkInfo
     * @param isException
     * @throws Exception
     * @throws InterruptedException
     */
    @SneakyThrows
    public void pollFlinkStatus(FlinkInfo flinkInfo, boolean isException) throws Exception,
            InterruptedException {
        if (isException) {
            delete(flinkInfo);
            throw new BusinessException("startup fail");
        }
        TimeUnit.SECONDS.sleep(15);
        while (true) {
            if (StringUtils.isNotEmpty(flinkInfo.getJobId())) {
                JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkInfo.getJobId());
                if (jobDetailsInfo == null) {
                    throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                            + String.format("Flink job %s not found", flinkInfo.getJobId()));
                }
                JobStatus jobStatus = flinkService.getJobStatus(flinkInfo.getJobId());
                if (jobStatus == INITIALIZING) {
                    log.info("poll Flink status");
                    Thread.sleep(2000L);
                    continue;
                }
                if (jobStatus == RUNNING) {
                    log.info("Flink status is Running");
                    break;
                }
                if (jobStatus == CANCELED || jobStatus == FAILED) {
                    delete(flinkInfo);
                    log.warn("flink job fail for status [{}]",jobStatus);
                    throw new BusinessException("startup fail");
                }
            }
        }
    }
}
