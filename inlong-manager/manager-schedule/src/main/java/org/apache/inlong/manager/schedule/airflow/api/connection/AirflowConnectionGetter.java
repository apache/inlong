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

import org.springframework.http.HttpMethod;

/**
 * Build call for AirflowConnectionGetter<br>
 * <table border="10">
 * <tr><th> Path Param </th><th> Description </th></tr>
 * <tr><td> connection_id </td><td> The connection ID. </td></tr>
 * </table>
 *
 * @http.response.details <table summary="Response Details" border="1">
 * <tr><th> Status Code </th><th> Description </th></tr>
 * <tr><td> 200 </td><td> Success. </td></tr>
 * <tr><td> 401 </td><td> Request not authenticated due to missing, invalid, authentication info. </td></tr>
 * <tr><td> 403 </td><td> Client does not have sufficient permission. </td></tr>
 * <tr><td> 404 </td><td> A specified resource is not found. </td></tr>
 * </table>
 */
public class AirflowConnectionGetter extends BaseAirflowApi<AirflowConnection> {

    public AirflowConnectionGetter(String connectionId) {
        pathParams.put("connection_id", connectionId);
    }

    @Override
    public HttpMethod getMethod() {
        return HttpMethod.GET;
    }

    @Override
    public String getPath() {
        return AirFlowAPIConstant.GET_CONNECTION_URI;
    }

    @Override
    public Class<AirflowConnection> getResponseType() {
        return AirflowConnection.class;
    }
}
