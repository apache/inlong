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

package org.apache.inlong.manager.schedule.airflow.api.dagruns;

import org.apache.inlong.manager.pojo.schedule.airflow.DAGRun;
import org.apache.inlong.manager.schedule.airflow.AirFlowAPIConstant;
import org.apache.inlong.manager.schedule.airflow.api.BaseAirflowApi;

import org.springframework.http.HttpMethod;

import java.util.Map;

/**
 * Build call for DAGRunsTrigger <br>
 * <table border="10">
 * <tr><th> Path Param </th><th> Description </th></tr>
 * <tr><td> dag_id </td><td> The DAG ID. </td></tr>
 * </table>
 *
 * <table border="10">
 * <tr><th> Request Body Param </th><th> Description </th></tr>
 * <tr>
 *     <td> conf </td>
 *     <td>
 *         JSON object describing additional configuration parameters. <br>
 *         The value of this field can be set only when creating the object. If you try to modify the field of an existing object, the request fails with an BAD_REQUEST error.<br>
 *     </td>
 * </tr>
 * <tr>
 *     <td> dag_run_id </td>
 *     <td> Run ID. <br>
 *         The value of this field can be set only when creating the object. If you try to modify the field of an existing object, the request fails with an BAD_REQUEST error. <br>
 *         If not provided, a value will be generated based on execution_date. <br>
 *         If the specified dag_run_id is in use, the creation request fails with an ALREADY_EXISTS error. <br>
 *         This together with DAG_ID are a unique key.<br>
 *     </td>
 * </tr>
 * <tr><td> data_interval_end </td><td> The end of the interval the DAG run covers. </td></tr>
 * <tr><td> data_interval_start </td><td> The beginning of the interval the DAG run covers. </td></tr>
 * <tr>
 *     <td> logical_date </td>
 *     <td>
 *         The logical date (previously called execution date). This is the time or interval covered by this DAG run, according to the DAG definition. <br>
 *         The value of this field can be set only when creating the object. If you try to modify the field of an existing object, the request fails with an BAD_REQUEST error.<br>
 *         This together with DAG_ID are a unique key. <br>
 *     </td>
 * </tr>
 * <tr><td> note </td><td> Contains manually entered notes by the user about the DagRun. </td></tr>
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

public class DAGRunsTrigger extends BaseAirflowApi<DAGRun> {

    public DAGRunsTrigger(String dagId) {
        this.pathParams.put("dag_id", dagId);
    }

    public DAGRunsTrigger(String dagId, Map<String, Object> requestBodyParams) {
        this.pathParams.put("dag_id", dagId);
        this.requestBodyParams = requestBodyParams;
    }

    @Override
    public HttpMethod getMethod() {
        return HttpMethod.POST;
    }

    @Override
    public String getPath() {
        return AirFlowAPIConstant.TRIGGER_NEW_DAG_RUN_URI;
    }

    @Override
    public Class<DAGRun> getResponseType() {
        return DAGRun.class;
    }
}
