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
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamPageRequest;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface InlongStreamApi {

    @POST("stream/save")
    Call<Response<Integer>> createStream(@Body InlongStreamInfo stream);

    @GET("stream/exist/{groupId}/{streamId}")
    Call<Response<Boolean>> isStreamExists(@Path("groupId") String groupId, @Path("streamId") String streamId);

    @POST("stream/update")
    Call<Response<Boolean>> updateStream(@Body InlongStreamInfo stream);

    @GET("stream/get")
    Call<Response<InlongStreamInfo>> getStream(@Query("groupId") String groupId,
            @Query("streamId") String streamId);

    @POST("stream/listAll")
    Call<Response<PageInfo<FullStreamResponse>>> listStream(@Body InlongStreamPageRequest request);

    @GET("stream/config/log/list")
    Call<Response<PageInfo<InlongStreamConfigLogListResponse>>> getStreamLogs(@Query("inlongGroupId") String groupId,
            @Query("inlongStreamId") String streamId);
}
