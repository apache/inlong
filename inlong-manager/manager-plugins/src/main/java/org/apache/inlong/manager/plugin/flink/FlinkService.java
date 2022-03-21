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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.inlong.manager.plugin.dto.FlinkConf;
import org.apache.inlong.manager.plugin.dto.JarRunRequestbody;
import org.apache.inlong.manager.plugin.dto.LoginConf;
import org.apache.inlong.manager.plugin.dto.StopWithSavepointRequestBody;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class FlinkService {
    String address;
    Integer port;
    private static String urlHead;

    public FlinkService(String address,Integer port) {
        this.address = address;
        this.port = port;
        this.urlHead = Constants.HTTP_URL +  address + Constants.SEPARATOR + port;

    }

    /**
     * get flink Client
     * @return
     * @throws Exception
     */
    public RestClusterClient<StandaloneClusterId> getFlinkClient() throws Exception {

        LoginConf loginConf = new LoginConf();
        loginConf.setRestPort(port);
        loginConf.setRestAddress(address);
        loginConf.setJobManagerPort(Constants.JOB_MANAGER_PORT);
        Configuration configuration = paseLoginConf(loginConf);
        RestClusterClient<StandaloneClusterId> client =
                new RestClusterClient<StandaloneClusterId>(configuration, StandaloneClusterId.getInstance());
        return client;

    }

    /**
     * get request
     * @param httpurl
     * @return
     */
    public static ResponseBody getReq(String httpurl) {
        Response response = null;
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .header("Authorization", "Client-ID " + UUID.randomUUID())
                .url(httpurl)
                .build();
        try {
            response = client.newCall(request).execute();
            log.info("get  req {}",response.body());
            return response.body();
        } catch (IOException e) {
            log.warn("get  req fail {}", e.getMessage());
        }
        return response.body();
    }

    /**
     * list jars
     * @param url
     * @return
     */
    public ResponseBody listJars(String url) {
        ResponseBody body = null;
        try {
            body = getReq(url);
            log.info("all jarfileinfo : {}",body);
            return body;
        } catch (Exception e) {
            log.error("fetch jars fail",e.getMessage());
        }
        return body;
    }

    /**
     * list all jobs
     * @return
     */
    public List<JobStatusMessage> listJobs() {

        List<JobStatusMessage> jobStatusMessageList = null;
        try {
            RestClusterClient<StandaloneClusterId> client = getFlinkClient();
            CompletableFuture<Collection<JobStatusMessage>> result = client.listJobs();
            jobStatusMessageList = new ArrayList<JobStatusMessage>(result.get());
            return jobStatusMessageList;

        } catch (Exception e) {
            log.error("fetch jobss fail",e.getMessage());
        }
        return jobStatusMessageList;
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
            e.printStackTrace();
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
            e.printStackTrace();
        }
        return jobDetailsInfo;
    }

    /**
     * submit job
     * @param flinkConf
     */
    public String submitJobs(FlinkConf flinkConf) {
        RestClusterClient<StandaloneClusterId> client = null;
        String localJarPath = flinkConf.getLocalJarPath();
        String[] programArgs = genProgramArgs(flinkConf);
        String jobId = "";
        try {
            LoginConf loginConf = new LoginConf();
            loginConf.setRestPort(port);
            loginConf.setRestAddress(address);
            loginConf.setJobManagerPort(Constants.JOB_MANAGER_PORT);
            Configuration configuration = paseLoginConf(loginConf);

            client = new RestClusterClient<StandaloneClusterId>(configuration, StandaloneClusterId.getInstance());
            int parallelism = Constants.PARALLELISM;
            File jarFile = new File(localJarPath);

            SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
            PackagedProgram program = PackagedProgram.newBuilder()
                    .setConfiguration(configuration)
                    .setEntryPointClassName(Constants.ENTRYPOINT_CLASS)
                    .setJarFile(jarFile)
                    .setArguments(programArgs)
                    .setSavepointRestoreSettings(savepointRestoreSettings).build();

            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program,configuration,parallelism,false);
            CompletableFuture<JobID> result = client.submitJob(jobGraph);
            jobId = result.get().toString();
            return jobId;
        } catch (Exception e) {
            log.error("submit job  error", e);
        }
        return jobId;
    }

    /**
     * stop job
     * @param jobId
     */
    public ResponseBody stopJobs(String jobId, StopWithSavepointRequestBody stopWithSavepointRequestBody) {
        ResponseBody responseBody = null;
        try {
            String httpUrl = urlHead + Constants.JOB_URL + Constants.URL_SEPARATOR + jobId + Constants.SUSPEND_URL;
            ObjectMapper objectMapper = new ObjectMapper();
            String requestBody = objectMapper.writeValueAsString(stopWithSavepointRequestBody);
            OkHttpClient client = new OkHttpClient();
            MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
            RequestBody body = RequestBody.create(mediaType, requestBody);
            Request request = new Request.Builder()
                    .url(httpUrl)
                    .method("POST", body)
                    .build();
            Response response = client.newCall(request).execute();
            log.info("the job : {} has stop",jobId);
            return response.body();
        } catch (Exception e) {
            log.error("stop job  error", e.getMessage());
        }
        return responseBody;
    }

    /**
     * trigger savepoint
     * @param jobId
     * @param triggerId
     * @return
     */
    public ResponseBody triggerSavepoint(String jobId,String triggerId) {
        String url = urlHead + Constants.JOB_URL + Constants.URL_SEPARATOR + jobId
                + Constants.SAVEPOINT + Constants.URL_SEPARATOR + triggerId;
        ResponseBody body = null;
        try {
            body = getReq(url);
            log.info("triggerSavepoint success");
            return body;
        } catch (Exception e) {
            log.error("triggerSavepoint after stop job fail",e.getMessage());
        }
        return body;
    }

    /**
     * upload jar
     * @param jarPath
     * @return
     * @throws Exception
     */
    public ResponseBody uploadJar(String jarPath,String fileName) throws Exception {
        String url = urlHead + Constants.JARS_URL + Constants.UPLOAD;
        OkHttpClient client = new OkHttpClient();
        RequestBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("file", fileName,
                        RequestBody.create(MediaType.parse("multipart/form-data"), new File(jarPath)))
                .build();

        Request request = new Request.Builder()
                .header("Authorization", "Client-ID " + UUID.randomUUID())
                .url(url)
                .post(requestBody)
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }
        return response.body();
    }

    /**
     * submit Jobs Without Jar
     * @param jarId
     * @param jarRunRequestbody
     * @return
     * @throws JsonProcessingException
     */
    public  ResponseBody submitJobsWithoutJar(String jarId,JarRunRequestbody jarRunRequestbody)
            throws JsonProcessingException {
        String httpUrl = urlHead + Constants.JARS_URL + jarId + Constants.RUN_URL;
        ObjectMapper objectMapper = new ObjectMapper();
        String requestBody = objectMapper.writeValueAsString(jarRunRequestbody);
        Response response = null;
        OkHttpClient client = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(mediaType, requestBody);
        Request request = new Request.Builder()
                .url(httpUrl)
                .method("POST", body)
                .build();
        try {
            response = client.newCall(request).execute();
            log.info("send remote req {}", response.body().string());
            return response.body();
        } catch (IOException e) {
            log.warn("send remote req fail {}", e.getMessage());
        }
        return response.body();
    }

    /**
     * cancel job
     * @param jobId
     */
    public void cancelJobs(String jobId) {
        try {
            RestClusterClient<StandaloneClusterId> client = getFlinkClient();
            JobID jobID = JobID.fromHexString(jobId);
            CompletableFuture<Acknowledge> result =
                    client.cancel(jobID);
            String stringId = result.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * delete job
     * @param jobId
     * @throws Exception
     */
    public  void deleteJobs(String jobId) throws Exception {
        String url = urlHead + Constants.JOB_URL + Constants.URL_SEPARATOR + jobId;
        OkHttpClient client = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(mediaType, "");
        Request request = new Request.Builder()
                .url(url)
                .method("PATCH", body)
                .build();
        try {
            client.newCall(request).execute();
            log.info("the jobId:{} delete successfuly",jobId);

        } catch (IOException e) {
            log.warn("the jobId:{} delete failure",jobId);
        }
    }

    /**
     * delete jar
     * @param jarId
     * @throws Exception
     */
    public void deleteJars(String jarId) throws Exception {
        String url = urlHead + Constants.JARS_URL + jarId;
        OkHttpClient client = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = null;
        Request request = new Request.Builder()
                .url(url)
                .method("DELETE", body)
                .build();
        try {
            client.newCall(request).execute();
            log.info("the jobId:{} delete successfuly",jarId);
        } catch (IOException e) {
            log.warn("the jobId:{} delete successfuly",jarId);
        }
    }

    /**
     * build the program of job
     * @param flinkConf
     * @return
     */
    private String[] genProgramArgs(FlinkConf flinkConf) {

        String[] data = new String[]{
                "-cluster-id",
                flinkConf.getJobName(),
                "-dataflow.info.file",
                flinkConf.getLocalConfPath(),
                "-source.type",
                flinkConf.getSourceType(),
                "-sink.type",
                flinkConf.getSinkType()
        };
        return data;
    }

    private Configuration paseLoginConf(LoginConf loginConf) {

        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, loginConf.getRestAddress());
        configuration.setInteger(JobManagerOptions.PORT, loginConf.getJobManagerPort());
        configuration.setInteger(RestOptions.PORT, loginConf.getRestPort());
        return configuration;
    }
}
