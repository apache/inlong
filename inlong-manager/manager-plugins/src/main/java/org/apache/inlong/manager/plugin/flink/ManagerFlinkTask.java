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
import org.apache.inlong.manager.plugin.dto.FlinkConf;
import org.apache.inlong.manager.plugin.flink.enums.BusinessExceptionDesc;
import org.apache.inlong.manager.plugin.flink.enums.TaskCommitType;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.JobStatus.CANCELED;
import static org.apache.flink.api.common.JobStatus.FAILED;
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

    public String start(FlinkConf flinkConf) throws IOException {
        try {
            TaskRunService.submit(new IntergrationTaskRunner(flinkService, flinkConf,
                    TaskCommitType.START_NOW.getCode()));
            return flinkConf.getJobId();
        } catch (Exception e) {
            log.warn("Flink job some exception [{}]", e.getMessage());
            throw new BusinessException(BusinessExceptionDesc.UnsupportedOperation
                    + e.getMessage());
        }
    }

    /**
     * @param flinkConf
     * @param dataflow
     */
    public void genPath(FlinkConf flinkConf, String dataflow) {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        path = path.substring(0, path.lastIndexOf(File.separator));
        String resource = "resource";
        String jarPath = path + File.separator + resource + File.separator + Constants.SORT_JAR;
        File file = new File(jarPath);
        if (!file.exists()) {
            log.warn("file path:[{}] not found sort jar", jarPath);
            throw new BusinessException(BusinessExceptionDesc.InternalError + " not found sort jar");
        }
        flinkConf.setLocalJarPath(jarPath);
        if (FlinkUtils.writeConfigToFile(path, flinkConf.getJobName(), dataflow)) {
            flinkConf.setLocalConfPath(path + File.separator + flinkConf.getJobName());
        } else {
            throw new BusinessException(BusinessExceptionDesc.InternalError + " write file fail");
        }
    }

    public void restart(FlinkConf flinkConf) throws Exception, IOException {
        JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkConf.getJobId());
        if (jobDetailsInfo == null) {
            throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                    + String.format("Flink job %s not found", flinkConf.getJobId()));
        }
        JobStatus jobStatus = flinkService.getJobStatus(flinkConf.getJobId());
        if (jobStatus == RUNNING) {
            TaskRunService.submit(new IntergrationTaskRunner(flinkService, flinkConf,
                    TaskCommitType.RESTART.getCode()));
        } else {
            throw new BusinessException(BusinessExceptionDesc.FailedOperation.getMessage()
                    + String.format("Flink job %s restart fail", flinkConf.getJobId()));
        }
    }

    public void stop(FlinkConf flinkConf) throws Exception {
        JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkConf.getJobId());
        if (jobDetailsInfo == null) {
            throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                    + String.format("Flink job %s not found", flinkConf.getJobId()));
        }
        JobStatus jobStatus = flinkService.getJobStatus(flinkConf.getJobId());
        if (jobStatus == RUNNING) {
            TaskRunService.submit(new IntergrationTaskRunner(flinkService, flinkConf,
                    TaskCommitType.STOP.getCode()));
        } else {
            throw new BusinessException(BusinessExceptionDesc.FailedOperation.getMessage()
                    + String.format("Flink job %s pause fail", flinkConf.getJobId()));
        }
    }

    public void delete(FlinkConf flinkConf) throws Exception {
        JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkConf.getJobId());
        if (jobDetailsInfo == null) {
            throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                    + String.format("Flink job %s not found", flinkConf.getJobId()));
        }
        JobStatus jobStatus = flinkService.getJobStatus(flinkConf.getJobId());
        switch (jobStatus) {
            case CANCELED:
                throw new BusinessException(BusinessExceptionDesc.UnsupportedOperation
                        + "not support delete when task has been canceled");
            case RUNNING:
                TaskRunService.submit(new IntergrationTaskRunner(flinkService, flinkConf,
                        TaskCommitType.DELETE.getCode()));
                break;
            default:
                throw new BusinessException(BusinessExceptionDesc.UnsupportedOperation
                        + "not support delete when task is OPERATING");
        }
    }

    @SneakyThrows
    public void pollFlinkStatus(FlinkConf flinkConf, boolean isException) throws Exception,
            InterruptedException {
        if (isException) {
            delete(flinkConf);
            throw new BusinessException("startup fail");
        }
        TimeUnit.SECONDS.sleep(15);
        while (true) {
            if (StringUtils.isNotEmpty(flinkConf.getJobId())) {
                JobDetailsInfo jobDetailsInfo = flinkService.getJobDetail(flinkConf.getJobId());
                if (jobDetailsInfo == null) {
                    throw new BusinessException(BusinessExceptionDesc.ResourceNotFound
                            + String.format("Flink job %s not found", flinkConf.getJobId()));
                }
                JobStatus jobStatus = flinkService.getJobStatus(flinkConf.getJobId());
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
                    delete(flinkConf);
                    log.warn("flink job fail for status [{}]",jobStatus);
                    throw new BusinessException("startup fail");
                }
            }
        }
    }
}
