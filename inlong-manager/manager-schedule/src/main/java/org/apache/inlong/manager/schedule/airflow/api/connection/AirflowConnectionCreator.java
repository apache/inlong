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

package org.apache.inlong.manager.schedule.airflow.api.connection;

import org.apache.inlong.manager.pojo.schedule.airflow.AirflowConnection;
import org.apache.inlong.manager.schedule.airflow.AirFlowAPIConstant;
import org.apache.inlong.manager.schedule.airflow.api.BaseAirflowApi;
import org.apache.inlong.manager.schedule.exception.AirflowScheduleException;

import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.springframework.http.HttpMethod;

import java.util.Map;

import static org.apache.inlong.manager.schedule.exception.AirflowScheduleException.AirflowErrorCode.BUILD_REQUEST_BODY_FAILED;

/**
 * Build call for AirflowConnectionGetter<br>
 * <table border="10">
 * <tr><th> Request Body Param </th><th> Description </th></tr>
 * <tr><td> connection_id </td><td> The connection ID. </td></tr>
 * <tr><td> conn_type </td><td> The connection type. </td></tr>
 * <tr><td> description </td><td> The description of the connection. </td></tr>
 * <tr><td> host </td><td> Host of the connection. </td></tr>
 * <tr><td> login </td><td> Login of the connection. </td></tr>
 * <tr><td> schema </td><td> Schema of the connection. </td></tr>
 * <tr><td> port </td><td> Port of the connection. </td></tr>
 * <tr><td> password </td><td> Password of the connection. </td></tr>
 * <tr><td> extra </td><td> Other values that cannot be put into another field, e.g. RSA keys.(optional) </td></tr>
 * </table>
 *
 * @http.response.details <table summary="Response Details" border="1">
 * <tr><th> Status Code </th><th> Description </th></tr>
 * <tr><td> 200 </td><td> Success. </td></tr>
 * <tr><td> 400 </td><td> Client specified an invalid argument. </td></tr>
 * <tr><td> 401 </td><td> Request not authenticated due to missing, invalid, authentication info. </td></tr>
 * <tr><td> 403 </td><td> Client does not have sufficient permission. </td></tr>
 * </table>
 */
@Slf4j
public class AirflowConnectionCreator extends BaseAirflowApi<AirflowConnection> {

    AirflowConnection connection = null;

    public AirflowConnectionCreator(AirflowConnection connection) {
        this.connection = connection;
    }

    public AirflowConnectionCreator(Map<String, Object> requestBodyParams) {
        this.requestBodyParams = requestBodyParams;
    }

    @Override
    public RequestBody getRequestBody() {
        if (connection != null) {
            try {
                return RequestBody.create(MediaType.parse("application/json; charset=utf-8"),
                        objectMapper.writeValueAsString(connection));
            } catch (Exception e) {
                log.error("Airflow request body construction failed: {}", e.getMessage(), e);
                throw new AirflowScheduleException(BUILD_REQUEST_BODY_FAILED,
                        String.format("Airflow request body construction failed: %s", e.getMessage()));
            }
        }
        return super.getRequestBody();
    }

    @Override
    public Class<AirflowConnection> getResponseType() {
        return AirflowConnection.class;
    }

    @Override
    public HttpMethod getMethod() {
        return HttpMethod.POST;
    }

    @Override
    public String getPath() {
        return AirFlowAPIConstant.LIST_CONNECTIONS_URI;
    }
}
