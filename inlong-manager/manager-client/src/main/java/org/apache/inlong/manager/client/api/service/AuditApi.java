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

package org.apache.inlong.manager.client.api.service;

import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRulePageRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.PUT;
import retrofit2.http.Path;

import java.util.List;

public interface AuditApi {

    @POST("audit/list")
    Call<Response<List<AuditVO>>> list(@Body AuditRequest auditRequest);

    @POST("audit/listAll")
    Call<Response<List<AuditVO>>> listAll(@Body AuditRequest auditRequest);

    @POST("audit/refreshCache")
    Call<Response<Boolean>> refreshCache();

    @POST("audit/alert/rule")
    Call<Response<Integer>> create(@Body AuditAlertRuleRequest request);

    @GET("audit/alert/rule/get/{id}")
    Call<Response<AuditAlertRule>> get(@Path("id") Integer id);

    @POST("audit/alert/rule/list")
    Call<Response<PageResult<AuditAlertRule>>> listByCondition(@Body AuditAlertRulePageRequest request);

    @PUT("audit/alert/rule/update")
    Call<Response<AuditAlertRule>> update(@Body AuditAlertRule rule);

    @DELETE("audit/delete/{id}")
    Call<Response<Boolean>> delete(@Path("id") Integer id);

}