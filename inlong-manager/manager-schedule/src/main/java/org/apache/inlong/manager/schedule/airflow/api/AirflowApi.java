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

package org.apache.inlong.manager.schedule.airflow.api;

import okhttp3.Request;
import okhttp3.RequestBody;
import org.springframework.http.HttpMethod;

import java.util.Map;
/**
 * Represents a generic interface for defining and constructing API requests to interact with Airflow.
 * This interface provides methods for specifying HTTP methods, endpoint paths, parameters,
 * request bodies, and constructing complete requests.
 * @param <T> the type of the response expected from the API, allowing flexibility for various response types.
 */
public interface AirflowApi<T> {

    /**
     * Get HTTP Method
     * @return HTTP Method
     */
    HttpMethod getMethod();

    /**
     * Get the requested path (relative to baseUrl)
     * @return Request path
     */
    String getPath();

    /**
     * Get path parameters to replace placeholders in the path (e.g. : "/api/v1/dags/{dag_id}/dagRuns")
     * @return Path parameter map
     */
    Map<String, String> getPathParams();

    /**
     * Get query parameters (e.g. "?Key=value")
     * @return GET parameter map
     */
    Map<String, Object> getQueryParams();

    /**
     * Get the request body (applicable to methods such as POST, PUT, etc.)
     * @return Post RequestBody Object
     */
    RequestBody getRequestBody();

    /**
     * Constructing a complete Request object
     * @param baseUrl Base URL
     * @return Constructed Request object
     */
    Request buildRequest(String baseUrl);

    /**
     * Returns the type of the response expected from this method.
     * @return The expected response type.
     */
    Class<T> getResponseType();
}
