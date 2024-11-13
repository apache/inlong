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

package org.apache.inlong.manager.schedule.dolphinscheduler;

import org.apache.inlong.common.bounded.BoundaryType;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.dolphinschedule.DSTaskDefinition;
import org.apache.inlong.manager.pojo.schedule.dolphinschedule.DSTaskParams;
import org.apache.inlong.manager.pojo.schedule.dolphinschedule.DSTaskRelation;
import org.apache.inlong.manager.pojo.schedule.dolphinschedule.DScheduleInfo;
import org.apache.inlong.manager.schedule.ScheduleUnit;
import org.apache.inlong.manager.schedule.exception.DolphinScheduleException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.core.util.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_CODE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PAGE_NO;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PAGE_SIZE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_SCHEDULE_TIME_FORMAT;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TASK_DESC;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TASK_GEN_NUM;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TASK_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TIMEZONE_ID;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_ID;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_ONLINE_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PAGE_NO;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PAGE_SIZE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROCESS_CODE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROCESS_DESC;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROCESS_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROJECT_DESC;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_PROJECT_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_RELEASE_STATE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_RELEASE_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_RESPONSE_DATA;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_RESPONSE_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_RESPONSE_TOTAL_LIST;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_SCHEDULE_DEF;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_SEARCH_VAL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TASK_DEFINITION;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TASK_GEN_NUM;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TASK_RELATION;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TOKEN;

/**
 * A utility class for interacting with DolphinScheduler API. This class includes methods
 * for creating, updating, and deleting projects, tasks, and process definitions in DolphinScheduler.
 * It also provides methods for generating schedules, calculating offsets, and executing HTTP requests.
 * This class leverages the OkHttp library for HTTP interactions and Gson for JSON parsing and serialization.
 */
public class DolphinScheduleUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DolphinScheduleEngine.class);

    private static final long MILLIS_IN_SECOND = 1000L;
    private static final long MILLIS_IN_MINUTE = 60 * MILLIS_IN_SECOND;
    private static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
    private static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;
    private static final long MILLIS_IN_WEEK = 7 * MILLIS_IN_DAY;
    private static final long MILLIS_IN_MONTH = 30 * MILLIS_IN_DAY;
    private static final long MILLIS_IN_YEAR = 365 * MILLIS_IN_DAY;
    private static final String HTTP_POST = "POST";
    private static final String HTTP_GET = "GET";
    private static final String HTTP_PUT = "PUT";
    private static final String HTTP_DELETE = "DELETE";
    private static final String CONTENT_TYPE = "Content-Type: application/json; charset=utf-8";
    private static final String SHELL_REQUEST_API = "/inlong/manager/api/group/submitOfflineJob";
    private final OkHttpClient client;
    private final ObjectMapper objectMapper;

    /**
     * Constructs a new instance of the DolphinScheduleUtils class.
     * Initializes the OkHttpClient and Gson instances.
     */
    public DolphinScheduleUtils() {
        this.client = new OkHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Checks the uniqueness of a project ID based on the given search value.
     *
     * @param url       The base URL of the DolphinScheduler API.
     * @param token     The authentication token to be used in the request header.
     * @param searchVal The name of the project to search for.
     * @return The unique project ID if found, or 0 if not found or an error occurs.
     */
    public long checkAndGetUniqueId(String url, String token, String searchVal) {

        Map<String, String> header = buildHeader(token);
        Map<String, String> queryParams = buildPageParam(searchVal);

        try {
            JsonObject response = executeHttpRequest(url, HTTP_GET, queryParams, header);

            if (response == null) {
                return 0;
            }
            JsonObject data = response.getAsJsonObject(DS_RESPONSE_DATA);

            if (data == null) {
                return 0;
            }
            JsonArray totalList = data.getAsJsonArray(DS_RESPONSE_TOTAL_LIST);
            // check uniqueness
            if (totalList != null && totalList.size() == 1) {
                JsonObject project = totalList.get(0).getAsJsonObject();
                String name = project.get(DS_RESPONSE_NAME).getAsString();
                if (name.equals(searchVal)) {
                    return project.get(DS_CODE).getAsLong();
                }
            }
            return 0;

        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in check id uniqueness: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in check id uniqueness: %s", e.getMessage()));
        }
    }

    /**
     * Creates a new project in DolphinScheduler.
     *
     * @param url         The base URL of the DolphinScheduler API.
     * @param token       The authentication token to be used in the request header.
     * @param projectName The name of the new project.
     * @param description The description of the new project.
     * @return The project code (ID) if creation is successful, or 0 if an error occurs.
     */
    public long creatNewProject(String url, String token, String projectName, String description) {
        Map<String, String> header = buildHeader(token);

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(DS_PROJECT_NAME, projectName);
        queryParams.put(DS_PROJECT_DESC, description);
        try {
            JsonObject response = executeHttpRequest(url, HTTP_POST, queryParams, header);

            if (response == null) {
                return 0;
            }
            JsonObject data = response.getAsJsonObject(DS_RESPONSE_DATA);
            if (data != null) {
                return data.get(DS_CODE).getAsLong();
            }
            return 0;
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in creating new project: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in creating new project: %s", e.getMessage()));
        }
    }

    public Map<Long, String> queryAllProcessDef(String url, String token) {
        Map<String, String> header = buildHeader(token);
        try {
            JsonObject response = executeHttpRequest(url, HTTP_GET, new HashMap<>(), header);

            if (response == null) {
                return null;
            }
            JsonArray data = response.getAsJsonArray(DS_RESPONSE_DATA);
            Map<Long, String> processDef = new HashMap<>();

            for (JsonElement processDefInfo : data) {
                processDef.put(processDefInfo.getAsJsonObject().get(DS_CODE).getAsLong(),
                        processDefInfo.getAsJsonObject().get(DS_PROCESS_NAME).getAsString());
            }
            LOGGER.info("Query all process definition success, processes info: {}", processDef);
            return processDef;
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in query process definition: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in query process definition: %s", e.getMessage()));
        }
    }

    /**
     * Generates a new task code in DolphinScheduler.
     *
     * @param url   The base URL of the DolphinScheduler API.
     * @param token The authentication token to be used in the request header.
     * @return The task code (ID) if generation is successful, or 0 if an error occurs.
     */
    public long genTaskCode(String url, String token) {
        Map<String, String> header = buildHeader(token);
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(DS_TASK_GEN_NUM, DS_DEFAULT_TASK_GEN_NUM);
        try {
            JsonObject response = executeHttpRequest(url, HTTP_GET, queryParams, header);

            if (response == null) {
                return 0;
            }
            JsonArray data = response.getAsJsonArray(DS_RESPONSE_DATA);
            if (data != null && data.size() == 1) {
                return data.get(0).getAsLong();
            }
            return 0;
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in generating task code: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in generating task code: %s", e.getMessage()));
        }
    }

    /**
     * Creates a process definition in DolphinScheduler.
     *
     * @param url         The base URL of the DolphinScheduler API.
     * @param token       The authentication token to be used in the request header.
     * @param name        The name of the process definition.
     * @param desc        The description of the process definition.
     * @param taskCode    The task code to be associated with this process definition.
     * @param host        The host where the process will run.
     * @param port        The port where the process will run.
     * @param username    The username for authentication.
     * @param password    The password for authentication.
     * @param offset      The offset for the scheduling.
     * @param groupId     The group ID of the process.
     * @return The process definition code (ID) if creation is successful, or 0 if an error occurs.
     */
    public long createProcessDef(String url, String token, String name, String desc, long taskCode, String host,
            int port, String username, String password, long offset, String groupId) throws JsonProcessingException {
        Map<String, String> header = buildHeader(token);

        DSTaskRelation taskRelation = new DSTaskRelation();
        taskRelation.setPostTaskCode(taskCode);
        String taskRelationJson = objectMapper.writeValueAsString(Collections.singletonList(taskRelation));

        DSTaskParams taskParams = new DSTaskParams();
        taskParams.setRawScript(buildScript(host, port, username, password, offset, groupId));

        DSTaskDefinition taskDefinition = new DSTaskDefinition();
        taskDefinition.setCode(taskCode);
        taskDefinition.setName(DS_DEFAULT_TASK_NAME);
        taskDefinition.setDescription(DS_DEFAULT_TASK_DESC);
        taskDefinition.setTaskParams(taskParams);
        String taskDefinitionJson = objectMapper.writeValueAsString(Collections.singletonList(taskDefinition));

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(DS_TASK_RELATION, taskRelationJson);
        queryParams.put(DS_TASK_DEFINITION, taskDefinitionJson);
        queryParams.put(DS_PROCESS_NAME, name);
        queryParams.put(DS_PROCESS_DESC, desc);

        try {
            JsonObject response = executeHttpRequest(url, HTTP_POST, queryParams, header);

            if (response != null) {
                JsonObject data = response.getAsJsonObject(DS_RESPONSE_DATA);
                if (data != null) {
                    return data.get(DS_CODE).getAsLong();
                }
            }
            return 0;
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in creating process definition: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in creating process definition: %s", e.getMessage()));
        }
    }

    /**
     * Releases a process definition in DolphinScheduler.
     *
     * @param processDefUrl The URL to release the process definition.
     * @param processDefCode The ID of the process definition.
     * @param token          The authentication token to be used in the request header.
     * @param status         The status to set for the process definition (e.g., "online" or "offline").
     * @return true if the process definition was successfully released, false otherwise.
     */
    public boolean releaseProcessDef(String processDefUrl, long processDefCode, String token, String status) {
        String url = processDefUrl + "/" + processDefCode + DS_RELEASE_URL;
        Map<String, String> header = buildHeader(token);
        Map<String, String> queryParam = new HashMap<>();
        queryParam.put(DS_RELEASE_STATE, status);

        try {
            JsonObject response = executeHttpRequest(url, HTTP_POST, queryParam, header);
            if (response != null && !response.get(DS_RESPONSE_DATA).isJsonNull()) {
                return response.get(DS_RESPONSE_DATA).getAsBoolean();
            }
            return false;
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in release process definition: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in release process definition: %s", e.getMessage()));
        }
    }

    /**
     * Create a schedule for process definition in DolphinScheduler.
     *
     * @param url The URL to create a schedule for the process definition.
     * @param processDefCode The ID of the process definition.
     * @param token          The authentication token to be used in the request header.
     * @param scheduleInfo    The schedule info
     * @return The schedule id
     */
    public int createScheduleForProcessDef(String url, long processDefCode, String token, ScheduleInfo scheduleInfo)
            throws JsonProcessingException {
        Map<String, String> header = buildHeader(token);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DS_DEFAULT_SCHEDULE_TIME_FORMAT);
        String startTime = scheduleInfo.getStartTime().toLocalDateTime()
                .atZone(ZoneId.of(DS_DEFAULT_TIMEZONE_ID)).format(formatter);
        String endTime = scheduleInfo.getEndTime().toLocalDateTime()
                .atZone(ZoneId.of(DS_DEFAULT_TIMEZONE_ID)).format(formatter);

        String crontab;
        if (scheduleInfo.getScheduleType() == 0) {
            crontab = generateCrontabExpression(scheduleInfo.getScheduleUnit(), scheduleInfo.getScheduleInterval());
        } else if (scheduleInfo.getScheduleType() == 1) {
            crontab = scheduleInfo.getCrontabExpression();
        } else {
            LOGGER.error("Unsupported schedule type: {}", scheduleInfo.getScheduleType());
            throw new DolphinScheduleException("Unsupported schedule type: " + scheduleInfo.getScheduleType());
        }

        DScheduleInfo dScheduleInfo = new DScheduleInfo();
        dScheduleInfo.setStartTime(startTime);
        dScheduleInfo.setEndTime(endTime);
        dScheduleInfo.setCrontab(crontab);
        dScheduleInfo.setTimezoneId(DS_DEFAULT_TIMEZONE_ID);
        String scheduleDef = objectMapper.writeValueAsString(dScheduleInfo);

        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(DS_PROCESS_CODE, String.valueOf(processDefCode));
        queryParams.put(DS_SCHEDULE_DEF, scheduleDef);

        try {
            JsonObject response = executeHttpRequest(url, HTTP_POST, queryParams, header);
            if (response == null) {
                return 0;
            }
            JsonObject data = response.get(DS_RESPONSE_DATA).getAsJsonObject();
            if (data != null) {
                return data.get(DS_ID).getAsInt();
            }
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in creating schedule for process definition: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in creating schedule for process definition: %s", e.getMessage()));
        }
        return 0;
    }

    /**
     * Online the schedule for process definition in DolphinScheduler.
     *
     * @param scheduleUrl The URL to online the schedule for process definition.
     * @param scheduleId The ID of the schedule of process definition.
     * @param token          The authentication token to be used in the request header.
     * @return whether online is succeeded
     */
    public boolean onlineScheduleForProcessDef(String scheduleUrl, int scheduleId, String token) {
        Map<String, String> header = buildHeader(token);
        String url = scheduleUrl + "/" + scheduleId + DS_ONLINE_URL;
        try {
            JsonObject response = executeHttpRequest(url, HTTP_POST, new HashMap<>(), header);
            if (response != null && !response.get(DS_RESPONSE_DATA).isJsonNull()) {
                return response.get(DS_RESPONSE_DATA).getAsBoolean();
            }
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in online process definition: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in online process definition: %s", e.getMessage()));
        }
        return false;
    }

    /**
     * Delete the process definition in DolphinScheduler.
     *
     * @param processDefUrl The URL to delete the process definition.
     * @param token          The authentication token to be used in the request header.
     * @param processDefCode          The process definition id
     */
    public void deleteProcessDef(String processDefUrl, String token, long processDefCode) {
        Map<String, String> header = buildHeader(token);
        String url = processDefUrl + "/" + processDefCode;
        try {
            executeHttpRequest(url, HTTP_DELETE, new HashMap<>(), header);
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in deleting process definition: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in deleting process definition: %s", e.getMessage()));
        }
    }

    /**
     * Delete the project by project code in DolphinScheduler.
     *
     * @param projectBaseUrl The URL to delete project
     * @param token          The authentication token to be used in the request header.
     * @param projectCode          The project code
     */
    public void deleteProject(String projectBaseUrl, String token, long projectCode) {
        Map<String, String> header = buildHeader(token);
        String url = projectBaseUrl + "/" + projectCode;
        try {
            executeHttpRequest(url, HTTP_DELETE, new HashMap<>(), header);
        } catch (IOException e) {
            LOGGER.error("Unexpected wrong in deleting project definition: ", e);
            throw new DolphinScheduleException(
                    String.format("Unexpected wrong in deleting project definition: %s", e.getMessage()));
        }
    }

    /**
     * Builds the header map for HTTP requests, including the authentication token.
     *
     * @param token The authentication token for the request.
     * @return A map representing the headers of the HTTP request.
     */
    private Map<String, String> buildHeader(String token) {
        Map<String, String> headers = new HashMap<>();
        headers.put(DS_TOKEN, token);
        return headers;
    }

    /**
     * Builds a query parameter map used for API calls that need to paginate or filter results.
     * This method can be used for searching projects or tasks.
     *
     * @param searchVal The value to search for.
     * @return A map containing the necessary query parameters.
     */
    private Map<String, String> buildPageParam(String searchVal) {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(DS_SEARCH_VAL, searchVal);
        queryParams.put(DS_PAGE_SIZE, DS_DEFAULT_PAGE_SIZE);
        queryParams.put(DS_PAGE_NO, DS_DEFAULT_PAGE_NO);
        return queryParams;
    }

    /**
     * Executes an HTTP request using OkHttp. Supports various HTTP methods (GET, POST, PUT, DELETE).
     *
     * @param url         The URL of the request.
     * @param method      The HTTP method (GET, POST, PUT, DELETE).
     * @param queryParams The query parameters for the request (optional).
     * @param headers     The headers for the request.
     * @return A JsonObject containing the response from the server.
     * @throws IOException If an I/O error occurs during the request.
     */
    @VisibleForTesting
    private JsonObject executeHttpRequest(String url, String method, Map<String, String> queryParams,
            Map<String, String> headers) throws IOException {
        // build param
        HttpUrl.Builder urlBuilder = HttpUrl.parse(url).newBuilder();

        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
        }
        HttpUrl httpUrl = urlBuilder.build();

        // build request
        Request.Builder requestBuilder = new Request.Builder()
                .url(httpUrl);

        // add header
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }
        RequestBody body = RequestBody.create(MediaType.parse(CONTENT_TYPE), "");
        // handle request method
        switch (method.toUpperCase()) {
            case HTTP_POST:
                requestBuilder.post(body);
                break;
            case HTTP_GET:
                requestBuilder.get();
                break;
            case HTTP_PUT:
                requestBuilder.put(body);
                break;
            case HTTP_DELETE:
                requestBuilder.delete(body);
                break;
            default:
                throw new IllegalArgumentException("Unsupported request method: " + method);
        }

        Request request = requestBuilder.build();

        // get response
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                String responseBody = response.body().string();
                return JsonParser.parseString(responseBody).getAsJsonObject();
            } else {
                LOGGER.error("Unexpected http response code: {}", response);
                throw new DolphinScheduleException("Unexpected http response code " + response);
            }
        }
    }

    /**
     * Calculate the offset according to schedule info
     *
     * @param scheduleInfo The schedule info
     * @return timestamp between two schedule task
     */
    public long calculateOffset(ScheduleInfo scheduleInfo) {
        if (scheduleInfo == null) {
            LOGGER.error("ScheduleInfo cannot be null");
            throw new DolphinScheduleException("ScheduleInfo cannot be null");
        }

        long offset = 0;

        // Determine offset based on schedule type
        if (scheduleInfo.getScheduleType() == null) {
            LOGGER.error("Schedule type cannot be null");
            throw new DolphinScheduleException("Schedule type cannot be null");
        }

        switch (scheduleInfo.getScheduleType()) {
            case 0: // Normal scheduling
                offset = calculateNormalOffset(scheduleInfo);
                break;
            case 1: // Crontab scheduling
                offset = calculateCronOffset(scheduleInfo);
                break;
            default:
                LOGGER.error("Invalid schedule type");
                throw new DolphinScheduleException("Invalid schedule type");
        }

        // Add delay time if specified
        if (scheduleInfo.getDelayTime() != null) {
            offset += scheduleInfo.getDelayTime() * MILLIS_IN_SECOND;
        }

        return offset;
    }

    private long calculateNormalOffset(ScheduleInfo scheduleInfo) {
        if (scheduleInfo.getScheduleInterval() == null || scheduleInfo.getScheduleUnit() == null) {
            LOGGER.error("Schedule interval and unit cannot be null for normal scheduling");
            throw new IllegalArgumentException("Schedule interval and unit cannot be null for normal scheduling");
        }
        switch (Objects.requireNonNull(ScheduleUnit.getScheduleUnit(scheduleInfo.getScheduleUnit()))) {
            case YEAR:
                return scheduleInfo.getScheduleInterval() * MILLIS_IN_YEAR;
            case MONTH:
                return scheduleInfo.getScheduleInterval() * MILLIS_IN_MONTH;
            case WEEK:
                return scheduleInfo.getScheduleInterval() * MILLIS_IN_WEEK;
            case DAY:
                return scheduleInfo.getScheduleInterval() * MILLIS_IN_DAY;
            case HOUR:
                return scheduleInfo.getScheduleInterval() * MILLIS_IN_HOUR;
            case MINUTE:
                return scheduleInfo.getScheduleInterval() * MILLIS_IN_MINUTE;
            case SECOND:
                return scheduleInfo.getScheduleInterval() * MILLIS_IN_SECOND;
            case ONE_ROUND:
                return scheduleInfo.getScheduleInterval();
            default:
                LOGGER.error("Invalid schedule unit");
                throw new DolphinScheduleException("Invalid schedule unit");
        }
    }

    private long calculateCronOffset(ScheduleInfo scheduleInfo) {
        if (scheduleInfo.getCrontabExpression() == null) {
            LOGGER.error("Crontab expression cannot be null for schedule type crontab");
            throw new DolphinScheduleException("Crontab expression cannot be null for schedule type crontab");
        }

        try {
            CronExpression cronExpression = new CronExpression(scheduleInfo.getCrontabExpression());
            Date firstExecution = cronExpression.getNextValidTimeAfter(new Date());
            Date secondExecution = cronExpression.getNextValidTimeAfter(firstExecution);

            if (secondExecution != null) {
                return secondExecution.getTime() - firstExecution.getTime();
            } else {
                LOGGER.error("Unable to calculate the next execution times for the cron expression");
                throw new DolphinScheduleException(
                        "Unable to calculate the next execution times for the cron expression");
            }
        } catch (Exception e) {
            LOGGER.error("Invalid cron expression: ", e);
            throw new DolphinScheduleException(String.format("Invalid cron expression: %s", e.getMessage()));
        }
    }

    private String generateCrontabExpression(String scheduleUnit, Integer scheduleInterval) {
        if (scheduleUnit.isEmpty()) {
            LOGGER.error("Schedule unit and interval must not be null for generating crontab expression");
            throw new DolphinScheduleException(
                    "Schedule unit and interval must not be null for generating crontab expression");
        }
        String crontabExpression;

        switch (Objects.requireNonNull(ScheduleUnit.getScheduleUnit(scheduleUnit))) {
            case SECOND:
                crontabExpression = String.format("0/%d * * * * ? *", scheduleInterval);
                break;
            case MINUTE:
                crontabExpression = String.format("* 0/%d * * * ? *", scheduleInterval);
                break;
            case HOUR:
                crontabExpression = String.format("* * 0/%d * * ? *", scheduleInterval);
                break;
            case DAY:
                crontabExpression = String.format("* * * 1/%d * ? *", scheduleInterval);
                break;
            case WEEK:
                crontabExpression = String.format("* * * 1/%d * ? *", scheduleInterval * 7);
                break;
            case MONTH:
                crontabExpression = String.format("* * * * 0/%d ? *", scheduleInterval);
                break;
            case YEAR:
                crontabExpression = String.format("* * * * * ? 0/%d", scheduleInterval);
                break;
            default:
                LOGGER.error("Unsupported schedule unit for generating crontab: {}", scheduleUnit);
                throw new DolphinScheduleException("Unsupported schedule unit for generating crontab: " + scheduleUnit);
        }

        return crontabExpression;
    }

    /**
     * Shell node in DolphinScheduler need to write in a script
     * When process definition schedule run, the shell node run,
     * Call back in inlong, sending a request with parameters required
     */
    private String buildScript(String host, int port, String username, String password, long offset, String groupId) {
        LOGGER.info("build script for host: {}, port: {}, username: {}, password: {}, offset: {}, groupId: {}", host,
                port, username, password, offset, groupId);
        return "#!/bin/bash\n\n" +

        // Get current timestamp
                "# Get current timestamp\n" +
                "lowerBoundary=$(date +%s)\n" +
                "echo \"get lowerBoundary: ${lowerBoundary}\"\n" +
                "upperBoundary=$(($lowerBoundary + " + offset + "))\n" +
                "echo \"get upperBoundary: ${upperBoundary}\"\n\n" +

                // Set URL
                "# Set URL and HTTP method\n" +
                "url=\"http://" + host + ":" + port + SHELL_REQUEST_API +
                "?username=" + username + "&password=" + password + "\"\n" +
                "echo \"get url: ${url}\"\n" +

                // Set HTTP method
                "httpMethod=\"" + HTTP_POST + "\"\n\n" +

                // Set request body
                "# Build request body\n" +
                "jsonBody=$(cat <<EOF\n" +
                "{\n" +
                "  \"boundaryType\": \"" + BoundaryType.TIME.getType() + "\",\n" +
                "  \"groupId\": \"" + groupId + "\",\n" +
                "  \"lowerBoundary\": \"${lowerBoundary}\",\n" +
                "  \"upperBoundary\": \"${upperBoundary}\"\n" +
                "}\n" +
                "EOF\n)\n\n" +
                "echo \"${jsonBody}\"\n\n" +

                // Send request
                "# Send request\n" +
                "response=$(curl -s -X \"$httpMethod\" \"$url\" \\\n" +
                "  -H \"" + CONTENT_TYPE + "\" \\\n" +
                "  -d \"$jsonBody\")\n\n" +

                // Log response
                "# Log response\n" +
                "echo \"Request Sending success, Response: $response\"";
    }

}
