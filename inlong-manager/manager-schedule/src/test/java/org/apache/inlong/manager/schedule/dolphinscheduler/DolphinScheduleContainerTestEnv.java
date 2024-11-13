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

import org.apache.inlong.manager.schedule.BaseScheduleTest;
import org.apache.inlong.manager.schedule.exception.DolphinScheduleException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinScheduleConstants.DS_RESPONSE_DATA;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_BASE_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_COOKIE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_COOKIE_SC_TYPE;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_COOKIE_SESSION_ID;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_DEFAULT_PASSWORD;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_DEFAULT_USERID;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_DEFAULT_USERNAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_EXPIRE_TIME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_EXPIRE_TIME_FORMAT;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_IMAGE_NAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_IMAGE_TAG;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_LOGIN_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_PASSWORD;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_RESPONSE_TOKEN;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_TOKEN_GEN_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_TOKEN_URL;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_USERID;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.DS_USERNAME;
import static org.apache.inlong.manager.schedule.dolphinscheduler.DolphinSchedulerContainerEnvConstants.HTTP_BASE_URL;

public abstract class DolphinScheduleContainerTestEnv extends BaseScheduleTest {

    protected static GenericContainer<?> dolphinSchedulerContainer;

    private static final Logger DS_LOG = LoggerFactory.getLogger(DolphinScheduleContainerTestEnv.class);

    // DS env generated final url and final token
    protected static String DS_URL;
    protected static String DS_TOKEN;

    public static void envSetUp() throws Exception {
        dolphinSchedulerContainer =
                new GenericContainer<>(DS_IMAGE_NAME + ":" + DS_IMAGE_TAG)
                        .withExposedPorts(12345, 25333)
                        .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
        dolphinSchedulerContainer.start();
        DS_URL = HTTP_BASE_URL + dolphinSchedulerContainer.getHost() + ":"
                + dolphinSchedulerContainer.getMappedPort(12345) + DS_BASE_URL;
        DS_LOG.info("DolphinScheduler is running at: {}", DS_URL);

        DS_TOKEN = accessToken();
        DS_LOG.info("DolphinScheduler token: {}", DS_TOKEN);
    }

    /**
     * This method just for DS testing, login by default admin username and password
     * generate a 1-day expiring token for test, the token will disappear with the DS container shutting down
     *
     * @return the DS token
     */
    private static String accessToken() {
        Map<String, String> loginParams = new HashMap<>();
        loginParams.put(DS_USERNAME, DS_DEFAULT_USERNAME);
        loginParams.put(DS_PASSWORD, DS_DEFAULT_PASSWORD);
        try {
            JsonObject loginResponse = executeHttpRequest(DS_URL + DS_LOGIN_URL, loginParams, new HashMap<>());
            if (loginResponse.get("success").getAsBoolean()) {
                String tokenGenUrl = DS_URL + DS_TOKEN_URL + DS_TOKEN_GEN_URL;
                Map<String, String> tokenParams = new HashMap<>();
                tokenParams.put(DS_USERID, String.valueOf(DS_DEFAULT_USERID));

                LocalDateTime now = LocalDateTime.now();
                LocalDateTime tomorrow = now.plusDays(1);
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DS_EXPIRE_TIME_FORMAT);
                String expireTime = tomorrow.format(formatter);
                tokenParams.put(DS_EXPIRE_TIME, expireTime);

                Map<String, String> cookies = new HashMap<>();
                cookies.put(DS_COOKIE_SC_TYPE, loginResponse.get(DS_RESPONSE_DATA)
                        .getAsJsonObject().get(DS_COOKIE_SC_TYPE).getAsString());
                cookies.put(DS_COOKIE_SESSION_ID, loginResponse.get(DS_RESPONSE_DATA)
                        .getAsJsonObject().get(DS_COOKIE_SESSION_ID).getAsString());

                JsonObject tokenGenResponse = executeHttpRequest(tokenGenUrl, tokenParams, cookies);

                String accessTokenUrl = DS_URL + DS_TOKEN_URL;
                tokenParams.put(DS_RESPONSE_TOKEN, tokenGenResponse.get(DS_RESPONSE_DATA).getAsString());
                JsonObject result = executeHttpRequest(accessTokenUrl, tokenParams, cookies);
                String token = result.get(DS_RESPONSE_DATA).getAsJsonObject().get(DS_RESPONSE_TOKEN).getAsString();
                DS_LOG.info("login and generate token success, token: {}", token);
                return token;
            }
            return null;
        } catch (Exception e) {
            DS_LOG.error("login and generate token fail: ", e);
            throw new DolphinScheduleException(String.format("login and generate token fail: %s", e.getMessage()));
        }
    }

    public static void envShutDown() {
        if (dolphinSchedulerContainer != null) {
            dolphinSchedulerContainer.close();
        }
    }

    private static JsonObject executeHttpRequest(String url, Map<String, String> queryParams,
            Map<String, String> cookies) throws IOException {
        OkHttpClient client = new OkHttpClient();

        // Build query parameters
        HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(url)).newBuilder();
        for (Map.Entry<String, String> entry : queryParams.entrySet()) {
            urlBuilder.addQueryParameter(entry.getKey(), entry.getValue());
        }
        HttpUrl httpUrl = urlBuilder.build();

        // Build the request
        Request.Builder requestBuilder = new Request.Builder()
                .url(httpUrl);

        // Add cookies to the request
        if (cookies != null && !cookies.isEmpty()) {
            String cookieHeader = cookies.entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining("; "));
            requestBuilder.header(DS_COOKIE, cookieHeader);
        }

        RequestBody body = RequestBody.create(MediaType.parse(CONTENT_TYPE), "");
        requestBuilder.post(body);

        Request request = requestBuilder.build();

        // Execute the request and parse the response
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                String responseBody = response.body().string();
                return JsonParser.parseString(responseBody).getAsJsonObject();
            } else {
                DS_LOG.error("Unexpected http response error: {}", response);
                throw new DolphinScheduleException("Unexpected http response error " + response);
            }
        }
    }

}
