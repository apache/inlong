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

import org.apache.inlong.manager.pojo.schedule.airflow.Error;
import org.apache.inlong.manager.schedule.airflow.api.AirflowApi;
import org.apache.inlong.manager.schedule.airflow.api.AirflowResponse;
import org.apache.inlong.manager.schedule.airflow.config.AirflowConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A unified class used for Airflow RESTful API processing.
 */
public class AirflowServerClient {

    private static final Logger logger = LoggerFactory.getLogger(AirflowServerClient.class);
    private final OkHttpClient httpClient;
    private final AirflowConfig config;
    private final ObjectMapper objectMapper;

    public AirflowServerClient(OkHttpClient httpClient, AirflowConfig config) {
        this.httpClient = httpClient;
        this.config = config;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Send request and parse response
     *
     * @param apiEndpoint apiEndpoint
     * @param <T>         Response to Generic Types
     * @return Parsed response object
     * @throws IOException Network request exception
     */
    public <T> AirflowResponse<T> sendRequest(AirflowApi<T> apiEndpoint) throws IOException {
        Request request = apiEndpoint.buildRequest(config.getBaseUrl());
        try (Response response = httpClient.newCall(request).execute()) {
            String responseBody = response.body().string();
            if (response.isSuccessful()) {
                return new AirflowResponse<>(true, objectMapper.readValue(responseBody, apiEndpoint.getResponseType()));
            } else {
                logger.error("Airflow Web API Request failed, status code: {} , detail: {}",
                        response.code(), objectMapper.readValue(responseBody, Error.class).getDetail());
                return new AirflowResponse<>(false, null);
            }
        }
    }
}
