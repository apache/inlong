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

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.workflow.EventLogResponse;
import org.apache.inlong.manager.pojo.workflow.WorkflowResult;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.util.Map;

public interface WorkflowApi {

    @Headers("Content-Type: application/json")
    @POST("workflow/approve/{taskId}")
    Call<Response<WorkflowResult>> startInlongGroup(@Path("taskId") Integer taskId, @Body Map<String, Object> request);

    @GET("workflow/event/list")
    Call<Response<PageInfo<EventLogResponse>>> getInlongGroupError(@Query("inlongGroupId") String groupId,
            @Query("status") Integer status);

}
