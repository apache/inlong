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

package org.apache.inlong.manager.client.api.inner.client;

import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.InlongClusterApi;
import org.apache.inlong.manager.client.api.service.InlongStreamApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamConfigLogListResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamPageRequest;
import org.apache.inlong.manager.common.util.Preconditions;

import java.util.List;

/**
 * Client for {@link InlongStreamApi}.
 */
public class InlongStreamClient {

    private final InlongStreamApi inlongStreamApi;

    public InlongStreamClient(ClientConfiguration configuration) {
        inlongStreamApi = ClientUtils.createRetrofit(configuration).create(InlongStreamApi.class);
    }

    /**
     * Create an inlong stream.
     */
    public Integer createStreamInfo(InlongStreamInfo streamInfo) {
        Response<Integer> response = ClientUtils.executeHttpCall(inlongStreamApi.createStream(streamInfo));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    public Boolean isStreamExists(InlongStreamInfo streamInfo) {
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        Preconditions.checkNotEmpty(groupId, "InlongGroupId should not be empty");
        Preconditions.checkNotEmpty(streamId, "InlongStreamId should not be empty");

        Response<Boolean> response = ClientUtils.executeHttpCall(inlongStreamApi.isStreamExists(groupId, streamId));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    public Pair<Boolean, String> updateStreamInfo(InlongStreamInfo streamInfo) {
        Response<Boolean> resp = ClientUtils.executeHttpCall(inlongStreamApi.updateStream(streamInfo));

        if (resp.getData() != null) {
            return Pair.of(resp.getData(), resp.getErrMsg());
        } else {
            return Pair.of(false, resp.getErrMsg());
        }
    }

    /**
     * Get inlong stream by the given groupId and streamId.
     */
    public InlongStreamInfo getStreamInfo(String groupId, String streamId) {
        Response<InlongStreamInfo> response = ClientUtils.executeHttpCall(inlongStreamApi.getStream(groupId, streamId));

        if (response.isSuccess()) {
            return response.getData();
        }
        if (response.getErrMsg().contains("not exist")) {
            return null;
        } else {
            throw new RuntimeException(response.getErrMsg());
        }
    }

    /**
     * Get inlong stream info.
     */
    public List<InlongStreamInfo> listStreamInfo(String inlongGroupId) {
        InlongStreamPageRequest pageRequest = new InlongStreamPageRequest();
        pageRequest.setInlongGroupId(inlongGroupId);

        Response<PageInfo<InlongStreamInfo>> response = ClientUtils.executeHttpCall(
                inlongStreamApi.listStream(pageRequest));
        ClientUtils.assertRespSuccess(response);
        return response.getData().getList();
    }

    /**
     * get inlong group error messages
     */
    public List<InlongStreamConfigLogListResponse> getStreamLogs(String inlongGroupId, String inlongStreamId) {
        Response<PageInfo<InlongStreamConfigLogListResponse>> response = ClientUtils.executeHttpCall(
                inlongStreamApi.getStreamLogs(inlongGroupId, inlongStreamId));
        ClientUtils.assertRespSuccess(response);
        return response.getData().getList();
    }
}
