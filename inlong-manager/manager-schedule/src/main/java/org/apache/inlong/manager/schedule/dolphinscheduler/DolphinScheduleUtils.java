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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_CODE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PAGE_NO;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_PAGE_SIZE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_RETRY_TIMES;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_SCHEDULE_TIME_FORMAT;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TASK_DESC;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TASK_GEN_NUM;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TASK_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_TIMEZONE_ID;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_DEFAULT_WAIT_MILLS;
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
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_SUCCESS;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TASK_DEFINITION;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TASK_GEN_NUM;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TASK_RELATION;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_TOKEN;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.DELETION_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.GEN_TASK_CODE_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.HTTP_REQUEST_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.INVALID_HTTP_METHOD;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.JSON_PARSE_ERROR;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.NETWORK_ERROR;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.PROCESS_DEFINITION_CREATION_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.PROCESS_DEFINITION_IN_USED_ERROR;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.PROCESS_DEFINITION_QUERY_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.PROCESS_DEFINITION_RELEASE_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.PROJECT_CREATION_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.SCHEDULE_CREATION_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.SCHEDULE_ONLINE_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.UNEXPECTED_ERROR;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.UNIQUE_CHECK_FAILED;
import static org.apache.inlong.manager.schedule.exception.DolphinScheduleException.UNSUPPORTED_SCHEDULE_TYPE;

/**
 * DolphinScheduler utils
 * A utility class for interacting with DolphinScheduler API.
 */
public class DolphinScheduleUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(DolphinScheduleEngine.class);

    private static final String POST = "POST";
    private static final String GET = "GET";
    private static final String DELETE = "DELETE";
    private static final long MILLIS_IN_SECOND = 1000L;
    private static final long MILLIS_IN_MINUTE = 60 * MILLIS_IN_SECOND;
    private static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
    private static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;
    private static final long MILLIS_IN_WEEK = 7 * MILLIS_IN_DAY;
    private static final long MILLIS_IN_MONTH = 30 * MILLIS_IN_DAY;
    private static final long MILLIS_IN_YEAR = 365 * MILLIS_IN_DAY;
    private static final String CONTENT_TYPE = "Content-Type: application/json; charset=utf-8";
    private static final String SHELL_REQUEST_API = "/inlong/manager/api/group/submitOfflineJob";
    private static final OkHttpClient CLIENT = new OkHttpClient();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DolphinScheduleUtils() {
    }

    /**
     * Checks the uniqueness of a project ID based on the given search value.
     *
     * @param url       The base URL of the DolphinScheduler API.
     * @param token     The authentication token to be used in the request header.
     * @param searchVal The name of the project to search for.
     * @return The unique project ID if found, or 0 if not found or an error occurs.
     */
    public static long checkAndGetUniqueId(String url, String token, String searchVal) {
        try {
            Map<String, String> header = buildHeader(token);
            Map<String, String> queryParams = buildPageParam(searchVal);

            JsonObject response = executeHttpRequest(url, GET, queryParams, header);

            JsonObject data = response.get(DS_RESPONSE_DATA).getAsJsonObject();
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
        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during checkAndGetUniqueId", e);
            throw new DolphinScheduleException(JSON_PARSE_ERROR,
                    String.format("Error parsing json during unique ID check for: %s at URL: %s", searchVal, url), e);

        } catch (DolphinScheduleException e) {
            LOGGER.error("DolphinScheduleException during unique ID check: {}", e.getDetailedMessage(), e);
            throw new DolphinScheduleException(UNIQUE_CHECK_FAILED,
                    String.format("Error checking unique ID for %s at URL: %s", searchVal, url));
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
    public static long creatProject(String url, String token, String projectName,
            String description) {
        try {
            Map<String, String> header = buildHeader(token);

            Map<String, String> queryParams = new HashMap<>();
            queryParams.put(DS_PROJECT_NAME, projectName);
            queryParams.put(DS_PROJECT_DESC, description);

            JsonObject response = executeHttpRequest(url, POST, queryParams, header);

            JsonObject data = response.get(DS_RESPONSE_DATA).getAsJsonObject();
            LOGGER.info("create project success, project data: {}", data);

            return data != null ? data.get(DS_CODE).getAsLong() : 0;
        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during creating project", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error creating project with name: %s and description: %s at URL: %s",
                            projectName, description, url));

        } catch (DolphinScheduleException e) {
            LOGGER.error("Creating project failed: {}", e.getMessage());
            throw new DolphinScheduleException(
                    PROJECT_CREATION_FAILED,
                    String.format("Error creating project with name: %s and description: %s at URL: %s",
                            projectName, description, url));
        }
    }

    /**
     * Query all process definition in project
     *
     * @param url The base URL of the DolphinScheduler API.
     * @param token The authentication token to be used in the request header.
     * @return Map of all the process definition
     */
    public static Map<Long, String> queryAllProcessDef(String url, String token) {
        Map<String, String> header = buildHeader(token);
        try {
            JsonObject response = executeHttpRequest(url, GET, new HashMap<>(), header);

            Map<Long, String> processDef =
                    StreamSupport.stream(response.get(DS_RESPONSE_DATA).getAsJsonArray().spliterator(), false)
                            .map(JsonElement::getAsJsonObject)
                            .collect(Collectors.toMap(
                                    jsonObject -> jsonObject.get(DS_CODE).getAsLong(),
                                    jsonObject -> jsonObject.get(DS_PROCESS_NAME).getAsString()));

            LOGGER.info("Query all process definition success, processes info: {}", processDef);
            return processDef;

        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during query all process definition", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error querying all process definitions at URL: %s", url));

        } catch (DolphinScheduleException e) {
            LOGGER.info("Query all process definition failed: {}", e.getMessage());
            throw new DolphinScheduleException(
                    PROCESS_DEFINITION_QUERY_FAILED,
                    String.format("Error querying all process definitions at URL: %s", url));
        }

    }

    /**
     * Generates a new task code in DolphinScheduler.
     *
     * @param url   The base URL of the DolphinScheduler API.
     * @param token The authentication token to be used in the request header.
     * @return The task code (ID) if generation is successful, or 0 if an error occurs.
     */
    public static long genTaskCode(String url, String token) {
        try {
            Map<String, String> header = buildHeader(token);

            Map<String, String> queryParams = new HashMap<>();
            queryParams.put(DS_TASK_GEN_NUM, DS_DEFAULT_TASK_GEN_NUM);

            JsonObject response = executeHttpRequest(url, GET, queryParams, header);

            JsonArray data = response.get(DS_RESPONSE_DATA).getAsJsonArray();

            LOGGER.info("Generate task code success, task code data: {}", data);
            return data != null && data.size() == 1 ? data.get(0).getAsLong() : 0;

        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during generate task code", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error generate task code at URL: %s", url));

        } catch (DolphinScheduleException e) {
            LOGGER.info("generate task code failed: {}", e.getMessage());
            throw new DolphinScheduleException(
                    GEN_TASK_CODE_FAILED,
                    String.format("Error generate task code at URL: %s", url));
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
    public static long createProcessDef(String url, String token, String name, String desc,
            long taskCode, String host,
            int port, String username, String password, long offset, String groupId) throws Exception {
        try {
            Map<String, String> header = buildHeader(token);

            DSTaskRelation taskRelation = new DSTaskRelation();
            taskRelation.setPostTaskCode(taskCode);
            String taskRelationJson = MAPPER.writeValueAsString(Collections.singletonList(taskRelation));

            DSTaskParams taskParams = new DSTaskParams();
            taskParams.setRawScript(buildScript(host, port, username, password, offset, groupId));

            DSTaskDefinition taskDefinition = new DSTaskDefinition();
            taskDefinition.setCode(taskCode);
            taskDefinition.setName(DS_DEFAULT_TASK_NAME);
            taskDefinition.setDescription(DS_DEFAULT_TASK_DESC);
            taskDefinition.setTaskParams(taskParams);
            String taskDefinitionJson = MAPPER.writeValueAsString(Collections.singletonList(taskDefinition));

            Map<String, String> queryParams = new HashMap<>();
            queryParams.put(DS_TASK_RELATION, taskRelationJson);
            queryParams.put(DS_TASK_DEFINITION, taskDefinitionJson);
            queryParams.put(DS_PROCESS_NAME, name);
            queryParams.put(DS_PROCESS_DESC, desc);

            JsonObject data = executeHttpRequest(url, POST, queryParams, header);

            LOGGER.info("create process definition success, process definition data: {}", data);
            return data != null ? data.get(DS_RESPONSE_DATA).getAsJsonObject().get(DS_CODE).getAsLong() : 0;
        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during creating process definition", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error creating process definition with name: %s and description: %s at URL: %s",
                            name, desc, url));

        } catch (DolphinScheduleException e) {
            throw new DolphinScheduleException(
                    PROCESS_DEFINITION_CREATION_FAILED,
                    String.format("Error creating process definition with name: %s and description: %s at URL: %s",
                            name, desc, url));
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
    public static boolean releaseProcessDef(String processDefUrl, long processDefCode,
            String token, String status) {
        try {
            String url = processDefUrl + "/" + processDefCode + DS_RELEASE_URL;
            Map<String, String> header = buildHeader(token);

            Map<String, String> queryParam = new HashMap<>();
            queryParam.put(DS_RELEASE_STATE, status);

            JsonObject response = executeHttpRequest(url, POST, queryParam, header);
            LOGGER.info("release process definition success, response data: {}", response);

            return response.get(DS_RESPONSE_DATA).getAsBoolean();

        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during releasing process definition", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error releasing process definition with code: %d and status: %s at URL: %s",
                            processDefCode, status, processDefUrl));

        } catch (DolphinScheduleException e) {
            throw new DolphinScheduleException(
                    PROCESS_DEFINITION_RELEASE_FAILED,
                    String.format("Error releasing process definition with code: %d and status: %s at URL: %s",
                            processDefCode, status, processDefUrl));
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
    public static int createScheduleForProcessDef(String url, long processDefCode,
            String token, ScheduleInfo scheduleInfo) throws Exception {

        try {
            Map<String, String> header = buildHeader(token);

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DS_DEFAULT_SCHEDULE_TIME_FORMAT);
            String startTime = scheduleInfo.getStartTime().toLocalDateTime()
                    .atZone(ZoneId.of(DS_DEFAULT_TIMEZONE_ID)).format(formatter);
            String endTime = scheduleInfo.getEndTime().toLocalDateTime()
                    .atZone(ZoneId.of(DS_DEFAULT_TIMEZONE_ID)).format(formatter);

            String crontab;
            switch (scheduleInfo.getScheduleType()) {
                case 0:
                    crontab = generateCrontabExpression(scheduleInfo.getScheduleUnit(),
                            scheduleInfo.getScheduleInterval());
                    break;

                case 1:
                    crontab = scheduleInfo.getCrontabExpression();
                    break;

                default:
                    LOGGER.error("Unsupported schedule type: {}", scheduleInfo.getScheduleType());
                    throw new DolphinScheduleException("Unsupported schedule type: " + scheduleInfo.getScheduleType());
            }

            DScheduleInfo dScheduleInfo = new DScheduleInfo();
            dScheduleInfo.setStartTime(startTime);
            dScheduleInfo.setEndTime(endTime);
            dScheduleInfo.setCrontab(crontab);
            dScheduleInfo.setTimezoneId(DS_DEFAULT_TIMEZONE_ID);
            String scheduleDef = MAPPER.writeValueAsString(dScheduleInfo);

            Map<String, String> queryParams = new HashMap<>();
            queryParams.put(DS_PROCESS_CODE, String.valueOf(processDefCode));
            queryParams.put(DS_SCHEDULE_DEF, scheduleDef);

            JsonObject response = executeHttpRequest(url, POST, queryParams, header);
            LOGGER.info("create schedule for process definition success, response data: {}", response);

            return response.get(DS_RESPONSE_DATA).getAsJsonObject().get(DS_ID).getAsInt();

        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during releasing process definition", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error creating schedule for process definition code: %d at URL: %s",
                            processDefCode, url));

        } catch (DolphinScheduleException e) {
            throw new DolphinScheduleException(
                    SCHEDULE_CREATION_FAILED,
                    String.format("Error creating schedule for process definition code: %d at URL: %s",
                            processDefCode, url));
        }
    }

    /**
     * Online the schedule for process definition in DolphinScheduler.
     *
     * @param scheduleUrl The URL to online the schedule for process definition.
     * @param scheduleId The ID of the schedule of process definition.
     * @param token          The authentication token to be used in the request header.
     * @return whether online is succeeded
     */
    public static boolean onlineScheduleForProcessDef(String scheduleUrl, int scheduleId,
            String token) {
        try {
            Map<String, String> header = buildHeader(token);

            String url = scheduleUrl + "/" + scheduleId + DS_ONLINE_URL;
            JsonObject response = executeHttpRequest(url, POST, new HashMap<>(), header);
            LOGGER.info("online schedule for process definition success, response data: {}", response);

            if (response != null && !response.get(DS_RESPONSE_DATA).isJsonNull()) {
                return response.get(DS_RESPONSE_DATA).getAsBoolean();
            }
            return false;

        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during online schedule", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error online schedule with ID: %d online at URL: %s", scheduleId, scheduleUrl));

        } catch (DolphinScheduleException e) {
            throw new DolphinScheduleException(
                    SCHEDULE_ONLINE_FAILED,
                    String.format("Error online schedule with ID: %d online at URL: %s", scheduleId, scheduleUrl));
        }
    }

    /**
     * Delete the process definition in DolphinScheduler.
     *
     * @param url The URL to delete the project or process definition.
     * @param token          The authentication token to be used in the request header.
     * @param code          The project code or process definition code
     */
    public static void delete(String url, String token, long code) {
        try {
            Map<String, String> header = buildHeader(token);

            String requestUrl = url + "/" + code;
            for (int retryTime = 1; retryTime <= DS_DEFAULT_RETRY_TIMES; retryTime++) {
                JsonObject response = executeHttpRequest(requestUrl, DELETE, new HashMap<>(), header);

                if (response.get(DS_SUCCESS).getAsBoolean()) {
                    LOGGER.info("Delete process or project success, response data: {}", response);
                    return;
                }

                if (response.get(DS_CODE).getAsInt() == PROCESS_DEFINITION_IN_USED_ERROR) {

                    if (retryTime == DS_DEFAULT_RETRY_TIMES) {
                        LOGGER.error(
                                "Maximum retry attempts reached for deleting process or project. URL: {}, Code: {}",
                                url, code);
                        throw new DolphinScheduleException(
                                DELETION_FAILED,
                                String.format("Failed to delete after %d retries. Code: %d at URL: %s",
                                        DS_DEFAULT_RETRY_TIMES, code, url));
                    }

                    LOGGER.warn("Attempt {} of {}, retrying after {} ms...", retryTime, DS_DEFAULT_RETRY_TIMES,
                            DS_DEFAULT_WAIT_MILLS);
                    Thread.sleep(DS_DEFAULT_WAIT_MILLS);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Thread interrupted while retrying delete process or project: ", e);
            throw new DolphinScheduleException(
                    DELETION_FAILED,
                    String.format("Thread interrupted while retrying delete for code: %d at URL: %s", code, url));
        } catch (JsonParseException e) {
            LOGGER.error("JsonParseException during deleting process or project", e);
            throw new DolphinScheduleException(
                    JSON_PARSE_ERROR,
                    String.format("Error deleting process or project with code: %d at URL: %s", code, url));

        } catch (DolphinScheduleException e) {
            LOGGER.error("Error deleting process or project: ", e);
            throw new DolphinScheduleException(
                    DELETION_FAILED,
                    String.format("Error deleting process or project with code: %d at URL: %s", code, url));
        }
    }

    /**
     * Builds the header map for HTTP requests, including the authentication token.
     *
     * @param token The authentication token for the request.
     * @return A map representing the headers of the HTTP request.
     */
    private static Map<String, String> buildHeader(String token) {
        Map<String, String> headers = new HashMap<>();
        if (StringUtils.isNotEmpty(token)) {
            headers.put(DS_TOKEN, token);
        }
        return headers;
    }

    /**
     * Builds a query parameter map used for API calls that need to paginate or filter results.
     * This method can be used for searching projects or tasks.
     *
     * @param searchVal The value to search for.
     * @return A map containing the necessary query parameters.
     */
    private static Map<String, String> buildPageParam(String searchVal) {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put(DS_SEARCH_VAL, searchVal);
        queryParams.put(DS_PAGE_SIZE, DS_DEFAULT_PAGE_SIZE);
        queryParams.put(DS_PAGE_NO, DS_DEFAULT_PAGE_NO);
        return queryParams;
    }

    /**
     * Calculate the offset according to schedule info
     *
     * @param scheduleInfo The schedule info
     * @return timestamp between two schedule task
     */
    public static long calculateOffset(ScheduleInfo scheduleInfo) {
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
                throw new DolphinScheduleException(
                        UNSUPPORTED_SCHEDULE_TYPE, "Invalid schedule type");
        }

        // Add delay time if specified
        if (scheduleInfo.getDelayTime() != null) {
            offset += scheduleInfo.getDelayTime() * MILLIS_IN_SECOND;
        }

        return offset;
    }

    private static long calculateNormalOffset(ScheduleInfo scheduleInfo) {
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

    private static long calculateCronOffset(ScheduleInfo scheduleInfo) {
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

    private static String generateCrontabExpression(String scheduleUnit, Integer scheduleInterval) {
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
     * Executes an HTTP request using OkHttp. Supports various HTTP methods (GET, POST, PUT, DELETE).
     *
     * @param url         The URL of the request.
     * @param method      The HTTP method (GET, POST, PUT, DELETE).
     * @param queryParams The query parameters for the request (optional).
     * @param headers     The headers for the request.
     * @return A JsonObject containing the response from the server.
     * @throws DolphinScheduleException If an error occurs during the request.
     */
    private static JsonObject executeHttpRequest(String url, String method, Map<String, String> queryParams,
            Map<String, String> headers) {
        HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(url)).newBuilder();

        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
        }
        HttpUrl httpUrl = urlBuilder.build();

        Request.Builder requestBuilder = new Request.Builder()
                .url(httpUrl);

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }
        RequestBody body = RequestBody.create(MediaType.parse(CONTENT_TYPE), "");

        switch (method.toUpperCase()) {
            case POST:
                requestBuilder.post(body);
                break;
            case GET:
                requestBuilder.get();
                break;
            case DELETE:
                requestBuilder.delete(body);
                break;
            default:
                throw new DolphinScheduleException(INVALID_HTTP_METHOD,
                        String.format("Unsupported request method: %s", method));
        }

        Request request = requestBuilder.build();

        // get response
        try (Response response = CLIENT.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : null;
            LOGGER.debug("HTTP request to {} completed with status code {}", httpUrl, response.code());

            if (response.isSuccessful() && responseBody != null) {
                return JsonParser.parseString(responseBody).getAsJsonObject();
            } else {
                LOGGER.error("HTTP request to {} failed. HTTP Status: {}, Response Body: {}", httpUrl, response.code(),
                        responseBody != null ? responseBody : "No response body");

                throw new DolphinScheduleException(
                        HTTP_REQUEST_FAILED,
                        String.format("HTTP request to %s failed. Status: %d, Response: %s",
                                httpUrl, response.code(), responseBody != null ? responseBody : "No response body"));
            }
        } catch (IOException e) {
            throw new DolphinScheduleException(
                    NETWORK_ERROR,
                    String.format("Network error during HTTP request to %s. Reason: %s", httpUrl, e.getMessage()));
        } catch (Exception e) {
            throw new DolphinScheduleException(
                    UNEXPECTED_ERROR,
                    String.format("Unexpected error during HTTP request to %s. Reason: %s", httpUrl, e.getMessage()));
        }
    }

    /**
     * Shell node in DolphinScheduler need to write in a script
     * When process definition schedule run, the shell node run,
     * Call back in inlong, sending a request with parameters required
     */
    private static String buildScript(String host, int port, String username, String password, long offset,
            String groupId) {
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
                "httpMethod=\"POST\"\n\n" +

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
