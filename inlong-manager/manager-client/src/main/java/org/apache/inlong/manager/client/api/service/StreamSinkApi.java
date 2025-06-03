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

import org.apache.inlong.manager.pojo.common.BatchResult;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.common.UpdateResult;
import org.apache.inlong.manager.pojo.sink.ParseFieldRequest;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.SinkPageRequest;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.TransformParseRequest;

import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

import java.util.List;
import java.util.Map;

public interface StreamSinkApi {

    @POST("sink/save")
    Call<Response<Integer>> save(@Body SinkRequest request);

    @POST("sink/batchSave")
    Call<Response<List<BatchResult>>> batchSave(@Body List<SinkRequest> requestList);

    @POST("sink/update")
    Call<Response<Boolean>> updateById(@Body SinkRequest request);

    @POST("sink/updateByKey")
    Call<Response<UpdateResult>> updateByKey(@Body SinkRequest request);

    @DELETE("sink/delete/{id}")
    Call<Response<Boolean>> deleteById(@Path("id") Integer id);

    @DELETE("sink/deleteByKey")
    Call<Response<Boolean>> deleteByKey(@Query("groupId") String groupId, @Query("streamId") String streamId,
            @Query("name") String name);

    @GET("sink/get/{id}")
    Call<Response<StreamSink>> get(@Path("id") Integer sinkId);

    @POST("sink/list")
    Call<Response<PageResult<StreamSink>>> list(@Body SinkPageRequest request);

    @POST("sink/parseFields")
    Call<Response<List<SinkField>>> parseFields(@Body ParseFieldRequest parseFieldRequest);

    @POST("sink/parseTransform")
    Call<Response<Map<String, Object>>> parseTransform(@Body TransformParseRequest request);
}
