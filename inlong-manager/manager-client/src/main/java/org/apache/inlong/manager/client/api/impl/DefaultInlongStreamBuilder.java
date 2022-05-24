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

package org.apache.inlong.manager.client.api.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.inner.InnerStreamContext;
import org.apache.inlong.manager.client.api.util.GsonUtils;
import org.apache.inlong.manager.client.api.util.InlongStreamSinkTransfer;
import org.apache.inlong.manager.client.api.util.InlongStreamSourceTransfer;
import org.apache.inlong.manager.client.api.util.InlongStreamTransfer;
import org.apache.inlong.manager.client.api.util.InlongStreamTransformTransfer;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.pojo.stream.StreamPipeline;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.StreamTransform;
import org.apache.inlong.manager.common.pojo.transform.TransformRequest;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;

import java.util.List;
import java.util.Map;

/**
 * Default inlong stream builder.
 */
public class DefaultInlongStreamBuilder extends InlongStreamBuilder {

    private InlongStreamImpl inlongStream;

    private InnerStreamContext streamContext;

    private InnerInlongManagerClient managerClient;

    public DefaultInlongStreamBuilder(
            InlongStreamConf streamConf,
            InnerGroupContext groupContext,
            InnerInlongManagerClient managerClient) {
        this.managerClient = managerClient;
        if (MapUtils.isEmpty(groupContext.getStreamContextMap())) {
            groupContext.setStreamContextMap(Maps.newHashMap());
        }
        InlongStreamInfo stream = InlongStreamTransfer.createStreamInfo(streamConf, groupContext.getGroupInfo());
        InnerStreamContext streamContext = new InnerStreamContext(stream);
        groupContext.setStreamContext(streamContext);
        this.streamContext = streamContext;
        this.inlongStream = new InlongStreamImpl(groupContext.getGroupInfo().getName(), stream.getName(),
                managerClient);
        if (CollectionUtils.isNotEmpty(streamConf.getStreamFields())) {
            this.inlongStream.setStreamFields(streamConf.getStreamFields());
        }
        groupContext.setStream(this.inlongStream);
    }

    @Override
    public InlongStreamBuilder source(StreamSource source) {
        inlongStream.addSource(source);
        SourceRequest sourceRequest = InlongStreamSourceTransfer.createSourceRequest(source,
                streamContext.getStreamInfo());
        streamContext.setSourceRequest(sourceRequest);
        return this;
    }

    @Override
    public InlongStreamBuilder sink(StreamSink sink) {
        inlongStream.addSink(sink);
        SinkRequest sinkRequest = InlongStreamSinkTransfer.createSinkRequest(sink, streamContext.getStreamInfo());
        streamContext.setSinkRequest(sinkRequest);
        return this;
    }

    @Override
    public InlongStreamBuilder fields(List<StreamField> fieldList) {
        inlongStream.setStreamFields(fieldList);
        List<InlongStreamFieldInfo> streamFieldInfos = InlongStreamTransfer.createStreamFields(fieldList,
                streamContext.getStreamInfo());
        streamContext.updateStreamFields(streamFieldInfos);
        return this;
    }

    @Override
    public InlongStreamBuilder transform(StreamTransform streamTransform) {
        inlongStream.addTransform(streamTransform);
        TransformRequest transformRequest = InlongStreamTransformTransfer.createTransformRequest(streamTransform,
                streamContext.getStreamInfo());
        streamContext.setTransformRequest(transformRequest);
        return this;
    }

    @Override
    public InlongStream init() {
        InlongStreamInfo streamInfo = streamContext.getStreamInfo();
        StreamPipeline streamPipeline = inlongStream.createPipeline();
        streamInfo.setExtParams(GsonUtils.toJson(streamPipeline));
        Double streamIndex = managerClient.createStreamInfo(streamInfo);
        streamInfo.setId(streamIndex.intValue());
        //Create source and update index
        List<SourceRequest> sourceRequests = Lists.newArrayList(streamContext.getSourceRequests().values());
        for (SourceRequest sourceRequest : sourceRequests) {
            sourceRequest.setId(managerClient.createSource(sourceRequest).intValue());
        }
        //Create sink and update index
        List<SinkRequest> sinkRequests = Lists.newArrayList(streamContext.getSinkRequests().values());
        for (SinkRequest sinkRequest : sinkRequests) {
            sinkRequest.setId(managerClient.createSink(sinkRequest).intValue());
        }
        //Create transform and update index
        List<TransformRequest> transformRequests = Lists.newArrayList(streamContext.getTransformRequests().values());
        for (TransformRequest transformRequest : transformRequests) {
            transformRequest.setId(managerClient.createTransform(transformRequest).intValue());
        }
        return inlongStream;
    }

    @Override
    public InlongStream initOrUpdate() {
        InlongStreamInfo dataStreamInfo = streamContext.getStreamInfo();
        StreamPipeline streamPipeline = inlongStream.createPipeline();
        dataStreamInfo.setExtParams(GsonUtils.toJson(streamPipeline));
        Boolean isExist = managerClient.isStreamExists(dataStreamInfo);
        if (isExist) {
            Pair<Boolean, String> updateMsg = managerClient.updateStreamInfo(dataStreamInfo);
            if (!updateMsg.getKey()) {
                throw new RuntimeException(String.format("Update data stream failed:%s", updateMsg.getValue()));
            }
            initOrUpdateTransform();
            initOrUpdateSource();
            initOrUpdateSink();
            return inlongStream;
        } else {
            return init();
        }
    }

    private void initOrUpdateTransform() {
        Map<String, TransformRequest> transformRequests = streamContext.getTransformRequests();
        InlongStreamInfo streamInfo = streamContext.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        List<TransformResponse> transformResponses = managerClient.listTransform(groupId, streamId);
        List<String> updateTransformNames = Lists.newArrayList();
        for (TransformResponse transformResponse : transformResponses) {
            StreamTransform transform = InlongStreamTransformTransfer.parseStreamTransform(transformResponse);
            final String transformName = transform.getTransformName();
            final int id = transformResponse.getId();
            if (transformRequests.get(transformName) == null) {
                TransformRequest transformRequest = InlongStreamTransformTransfer.createTransformRequest(transform,
                        streamInfo);
                boolean isDelete = managerClient.deleteTransform(transformRequest);
                if (!isDelete) {
                    throw new RuntimeException(String.format("Delete transform=%s failed", transformRequest));
                }
            } else {
                TransformRequest transformRequest = transformRequests.get(transformName);
                transformRequest.setId(id);
                Pair<Boolean, String> updateState = managerClient.updateTransform(transformRequest);
                if (!updateState.getKey()) {
                    throw new RuntimeException(String.format("Update transform=%s failed with err=%s", transformRequest,
                            updateState.getValue()));
                }
                transformRequest.setId(transformResponse.getId());
                updateTransformNames.add(transformName);
            }
        }
        for (Map.Entry<String, TransformRequest> requestEntry : transformRequests.entrySet()) {
            String transformName = requestEntry.getKey();
            if (updateTransformNames.contains(transformName)) {
                continue;
            }
            TransformRequest transformRequest = requestEntry.getValue();
            transformRequest.setId(managerClient.createTransform(transformRequest).intValue());
        }
    }

    private void initOrUpdateSource() {
        Map<String, SourceRequest> sourceRequests = streamContext.getSourceRequests();
        InlongStreamInfo streamInfo = streamContext.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        List<SourceListResponse> sourceListResponses = managerClient.listSources(groupId, streamId);
        List<String> updateSourceNames = Lists.newArrayList();
        for (SourceListResponse sourceListResponse : sourceListResponses) {
            final String sourceName = sourceListResponse.getSourceName();
            final int id = sourceListResponse.getId();
            if (sourceRequests.get(sourceName) == null) {
                boolean isDelete = managerClient.deleteSource(id);
                if (!isDelete) {
                    throw new RuntimeException(String.format("Delete source=%s failed", sourceListResponse));
                }
            } else {
                SourceRequest sourceRequest = sourceRequests.get(sourceName);
                sourceRequest.setId(id);
                Pair<Boolean, String> updateState = managerClient.updateSource(sourceRequest);
                if (!updateState.getKey()) {
                    throw new RuntimeException(String.format("Update source=%s failed with err=%s", sourceRequest,
                            updateState.getValue()));
                }
                updateSourceNames.add(sourceName);
                sourceRequest.setId(sourceListResponse.getId());
            }
        }
        for (Map.Entry<String, SourceRequest> requestEntry : sourceRequests.entrySet()) {
            String sourceName = requestEntry.getKey();
            if (updateSourceNames.contains(sourceName)) {
                continue;
            }
            SourceRequest sourceRequest = requestEntry.getValue();
            sourceRequest.setId(managerClient.createSource(sourceRequest).intValue());
        }
    }

    private void initOrUpdateSink() {
        Map<String, SinkRequest> sinkRequests = streamContext.getSinkRequests();
        InlongStreamInfo streamInfo = streamContext.getStreamInfo();
        final String groupId = streamInfo.getInlongGroupId();
        final String streamId = streamInfo.getInlongStreamId();
        List<SinkListResponse> sinkListResponses = managerClient.listSinks(groupId, streamId);
        List<String> updateSinkNames = Lists.newArrayList();
        for (SinkListResponse sinkListResponse : sinkListResponses) {
            final String sinkName = sinkListResponse.getSinkName();
            final int id = sinkListResponse.getId();
            if (sinkRequests.get(sinkName) == null) {
                boolean isDelete = managerClient.deleteSink(id);
                if (!isDelete) {
                    throw new RuntimeException(String.format("Delete sink=%s failed", sinkListResponse));
                }
            } else {
                SinkRequest sinkRequest = sinkRequests.get(sinkName);
                sinkRequest.setId(id);
                Pair<Boolean, String> updateState = managerClient.updateSink(sinkRequest);
                if (!updateState.getKey()) {
                    throw new RuntimeException(String.format("Update sink=%s failed with err=%s", sinkRequest,
                            updateState.getValue()));
                }
                updateSinkNames.add(sinkName);
                sinkRequest.setId(sinkListResponse.getId());
            }
        }
        for (Map.Entry<String, SinkRequest> requestEntry : sinkRequests.entrySet()) {
            String sinkName = requestEntry.getKey();
            if (updateSinkNames.contains(sinkName)) {
                continue;
            }
            SinkRequest sinkRequest = requestEntry.getValue();
            sinkRequest.setId(managerClient.createSink(sinkRequest).intValue());
        }
    }
}
