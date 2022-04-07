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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.plugin.flink.dto.FlinkConfig;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.plugin.flink.dto.StopWithSavepointRequestBody;
import org.apache.inlong.manager.plugin.util.FlinkConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class FlinkService {

    private static final Pattern numberPattern = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)");

    private final FlinkConfig flinkConfig;
    private final Integer port;
    private final Integer jobManagerPort;
    private final String  address;
    private final Integer parallelism;
    private final String savepointDirectory;

    public FlinkService(String endpoint) throws IOException {
        FlinkConfiguration flinkConfiguration = new FlinkConfiguration();
        flinkConfig = flinkConfiguration.getFlinkConfig();
        jobManagerPort = flinkConfig.getJobManagerPort();
        parallelism = flinkConfig.getParallelism();
        savepointDirectory = flinkConfig.getSavepointDirectory();
        if (StringUtils.isEmpty(endpoint)) {
            address = flinkConfig.getAddress();
            port = flinkConfig.getPort();
        } else {
            address = translateFromEndpont(endpoint).get("address");
            port = Integer.valueOf(translateFromEndpont(endpoint).get("port"));
        }
    }

    /**
     * translate  Endpont to address & port
     *
     * @param endpoint
     * @return
     */
    private Map<String, String> translateFromEndpont(String endpoint) {
        Map<String, String> map = new HashMap<>(2);
        try {
            Matcher matcher = numberPattern.matcher(endpoint);
            while (matcher.find()) {
                map.put("address", matcher.group(1));
                map.put("port", matcher.group(2));
                return map;
            }
        } catch (Exception e) {
            log.error("fetch addres:port fail", e.getMessage());
        }
        return map;
    }

    /**
     * get flinkConfig
     * @return
     */
    public FlinkConfig getFlinkConfig() {
        return flinkConfig;
    }

    /**
     * get flink Client
     * @return
     * @throws Exception
     */
    public RestClusterClient<StandaloneClusterId> getFlinkClient() throws Exception {
        Configuration configuration = initConfiguration();
        RestClusterClient<StandaloneClusterId> client =
                new RestClusterClient<StandaloneClusterId>(configuration, StandaloneClusterId.getInstance());
        return client;

    }

    /**
     * init flink-client Configuration
     * @return
     */
    public Configuration initConfiguration()  {
        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, address);
        configuration.setInteger(JobManagerOptions.PORT, jobManagerPort);
        configuration.setInteger(RestOptions.PORT, port);
        return configuration;

    }

    /**
     * get job status
     * @return
     */
    public JobStatus getJobStatus(String jobId) {
        JobStatus status = null;
        try {
            RestClusterClient<StandaloneClusterId> client = getFlinkClient();
            JobID jobID = JobID.fromHexString(jobId);
            CompletableFuture<JobStatus> jobStatus = client.getJobStatus(jobID);
            status = jobStatus.get();
            return status;

        } catch (Exception e) {
            log.error("get job status error", e.getMessage());
        }
        return status;
    }

    /**
     * get job detail
     * @return
     */
    public JobDetailsInfo getJobDetail(String jobId) {
        JobDetailsInfo jobDetailsInfo = null;
        try {
            RestClusterClient<StandaloneClusterId> client = getFlinkClient();
            JobID jobID = JobID.fromHexString(jobId);
            CompletableFuture<JobDetailsInfo> jobDetails = client.getJobDetails(jobID);
            jobDetailsInfo = jobDetails.get();
            return jobDetailsInfo;

        } catch (Exception e) {
            log.error("get job detail error", e.getMessage());
        }
        return jobDetailsInfo;
    }

    /**
     * submit job
     * @param flinkInfo
     */
    public String submitJobs(FlinkInfo flinkInfo) {
        RestClusterClient<StandaloneClusterId> client = null;
        String localJarPath = flinkInfo.getLocalJarPath();
        String[] programArgs = genProgramArgs(flinkInfo);
        String jobId = "";
        try {
            client = getFlinkClient();
            Configuration configuration = initConfiguration();
            File jarFile = new File(localJarPath);
                SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
                PackagedProgram program = PackagedProgram.newBuilder()
                        .setConfiguration(configuration)
                        .setEntryPointClassName(Constants.ENTRYPOINT_CLASS)
                        .setJarFile(jarFile)
                        .setArguments(programArgs)
                        .setSavepointRestoreSettings(savepointRestoreSettings).build();
                JobGraph jobGraph =
                        PackagedProgramUtils.createJobGraph(program,configuration,parallelism,false);
                CompletableFuture<JobID> result = client.submitJob(jobGraph);
                jobId = result.get().toString();
                return jobId;
        } catch (Exception e) {
            log.error("submit job error", e.getMessage());
        }
        return jobId;
    }

    /**
     * restore job with savepoint
     * @param flinkInfo
     */
    public String restore(FlinkInfo flinkInfo) {
        RestClusterClient<StandaloneClusterId> client = null;
        String localJarPath = flinkInfo.getLocalJarPath();
        String[] programArgs = genProgramArgs(flinkInfo);
        String jobId = "";
        try {
            client = getFlinkClient();
            Configuration configuration = initConfiguration();
            File jarFile = new File(localJarPath);
            if (StringUtils.isNotEmpty(flinkInfo.getSavepointPath())) {
                SavepointRestoreSettings savepointRestoreSettings =
                        SavepointRestoreSettings.forPath(savepointDirectory,false);
                PackagedProgram program = PackagedProgram.newBuilder()
                        .setConfiguration(configuration)
                        .setEntryPointClassName(Constants.ENTRYPOINT_CLASS)
                        .setJarFile(jarFile)
                        .setArguments(programArgs)
                        .setSavepointRestoreSettings(savepointRestoreSettings).build();
                JobGraph jobGraph =
                        PackagedProgramUtils.createJobGraph(program,configuration,parallelism,false);
                CompletableFuture<JobID> result = client.submitJob(jobGraph);
                jobId = result.get().toString();
                return jobId;
            }
        } catch (Exception e) {
            log.error("restore job error", e.getMessage());
        }
        return jobId;
    }

    /**
     * stop job with savepoint
     * @param jobId
     * @param requestBody
     * @return
     */
    public String stopJobs(String jobId, StopWithSavepointRequestBody requestBody) {
        String result = null;
        try {
            RestClusterClient<StandaloneClusterId> client = getFlinkClient();
            JobID jobID = JobID.fromHexString(jobId);
            CompletableFuture<String> stopResult =
                    client.stopWithSavepoint(jobID,requestBody.isDrain(),requestBody.getTargetDirectory());
            result = stopResult.get();
            return result;

        } catch (Exception e) {
            log.error("stop job error", e.getMessage());
        }
        return result;
    }

    /**
     * cancel job
     * @param jobId
     * @return
     */
    public void cancelJobs(String jobId) {
        try {
            RestClusterClient<StandaloneClusterId> client = getFlinkClient();
            JobID jobID = JobID.fromHexString(jobId);
            CompletableFuture<Acknowledge> result = client.cancel(jobID);
        } catch (Exception e) {
            log.error("cancel job error", e.getMessage());
        }
    }

    /**
     * build the program of job
     * @param flinkInfo
     * @return
     */
    private String[] genProgramArgs(FlinkInfo flinkInfo) {
        List<String> list =  new ArrayList<>();
        list.add("-cluster-id");
        list.add(flinkInfo.getJobName());
        list.add("-dataflow.info.file");
        list.add(flinkInfo.getLocalConfPath());
        list.add("-source.type");
        list.add(flinkInfo.getSourceType());
        list.add("-sink.type");
        list.add(flinkInfo.getSinkType());
        // one group one stream now
        if (flinkInfo.getInlongStreamInfoList() != null
                && !flinkInfo.getInlongStreamInfoList().isEmpty()) {
            InlongStreamInfo inlongStreamInfo = flinkInfo.getInlongStreamInfoList().get(0);
            list.add("-job.orderly.output");
            list.add(String.valueOf(inlongStreamInfo.getSyncSend()));
        }
        String[] data = list.toArray(new String[list.size()]);
        return data;
    }
}
