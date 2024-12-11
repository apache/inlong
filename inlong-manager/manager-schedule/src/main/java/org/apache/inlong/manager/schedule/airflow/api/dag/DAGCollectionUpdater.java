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

package org.apache.inlong.manager.schedule.airflow.api.dag;

import org.apache.inlong.manager.pojo.schedule.airflow.DAGCollection;
import org.apache.inlong.manager.schedule.airflow.AirFlowAPIConstant;
import org.apache.inlong.manager.schedule.airflow.api.BaseAirflowApi;

import org.springframework.http.HttpMethod;

import java.util.Map;

/**
 * Build call for DAGCollectionUpdater< br>
 * <table border="10">
 * <tr><th> GET Param </th><th> Description </th></tr>
 * <tr><td> limit </td><td> The numbers of items to return. (optional, default to 100) </td></tr>
 * <tr><td> offset </td><td> The number of items to skip before starting to collect the result set. (optional) </td></tr>
 * <tr><td> tags </td><td> List of tags to filter results.(optional) </td></tr>
 * <tr><td> update_mask </td><td> The fields to update on the resource. If absent or empty, all modifiable fields are updated. A comma-separated list of fully qualified names of fields.(optional) </td></tr>
 * <tr><td> only_active </td><td> Only filter active DAGs. (optional, default to true) </td></tr>
 * <tr><td> dag_id_pattern </td><td> If set, only return DAGs with dag_ids matching this pattern. (required) </td></tr>
 * </table>
 *
 * <table border="10">
 * <tr><th> Request Body Param </th><th> Description </th></tr>
 * <tr><td> is_paused </td><td> Whether the DAG is paused. </td></tr>
 * </table>
 *
 * @http.response.details <table summary="Response Details" border="1">
 * <tr><th> Status Code </th><th> Description </th></tr>
 * <tr><td> 200 </td><td> Success. </td><td></tr>
 * <tr><td> 401 </td><td> Request not authenticated due to missing, invalid, authentication info. </td><td></tr>
 * <tr><td> 403 </td><td> Client does not have sufficient permission. </td><td></tr>
 * <tr><td> 404 </td><td> A specified resource is not found. </td><td></tr>
 * </table>
 */
public class DAGCollectionUpdater extends BaseAirflowApi<DAGCollection> {

    public DAGCollectionUpdater(String dagIdPattern, boolean isPaused) {
        this.queryParams.put("dag_id_pattern", dagIdPattern);
        this.requestBodyParams.put("is_paused", isPaused);
    }

    public DAGCollectionUpdater(Map<String, Object> queryParams, Map<String, Object> requestBodyParams) {
        this.queryParams = queryParams;
        this.requestBodyParams = requestBodyParams;
    }

    @Override
    public HttpMethod getMethod() {
        return HttpMethod.PATCH;
    }

    @Override
    public String getPath() {
        return AirFlowAPIConstant.LIST_DAGS_URI;
    }

    @Override
    public Class<DAGCollection> getResponseType() {
        return DAGCollection.class;
    }
}
