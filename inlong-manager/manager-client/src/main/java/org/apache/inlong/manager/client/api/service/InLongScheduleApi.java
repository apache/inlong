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

import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;
import org.apache.inlong.manager.pojo.schedule.ScheduleInfoRequest;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface InLongScheduleApi {

    @POST("schedule/save")
    Call<Response<Integer>> createSchedule(@Body ScheduleInfoRequest request);

    @GET("schedule/exist/{groupId}")
    Call<Response<Boolean>> exist(@Path("groupId") String groupId);

    @POST("schedule/update")
    Call<Response<Boolean>> update(@Body ScheduleInfoRequest request);

    @GET("schedule/get")
    Call<Response<ScheduleInfo>> get(@Query("groupId") String groupId);

    @DELETE("schedule/delete/{groupId}")
    Call<Response<Boolean>> delete(@Path("groupId") String groupId);

}
