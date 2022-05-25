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
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.util.GsonUtils;
import org.apache.inlong.manager.client.api.util.InlongStreamSinkTransfer;
import org.apache.inlong.manager.client.api.util.InlongStreamSourceTransfer;
import org.apache.inlong.manager.client.api.util.InlongStreamTransfer;
import org.apache.inlong.manager.client.api.util.InlongStreamTransformTransfer;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamField;
import org.apache.inlong.manager.common.pojo.stream.StreamNodeRelationship;
import org.apache.inlong.manager.common.pojo.stream.StreamPipeline;
import org.apache.inlong.manager.common.pojo.stream.StreamSink;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.StreamTransform;
import org.apache.inlong.manager.common.pojo.transform.TransformRequest;
import org.apache.inlong.manager.common.pojo.transform.TransformResponse;
import org.apache.inlong.manager.common.util.AssertUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Inlong stream service implementation.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class InlongStreamImpl extends InlongStream {

    private InnerInlongManagerClient managerClient;

    private String groupId;

    private String streamId;

    private Map<String, StreamSource> streamSources = Maps.newHashMap();

    private Map<String, StreamSink> streamSinks = Maps.newHashMap();

    private Map<String, StreamTransform> streamTransforms = Maps.newHashMap();

    private List<StreamField> streamFields = Lists.newArrayList();

    /**
     * Constructor of InlongStreamImpl.
     */
    public InlongStreamImpl(FullStreamResponse fullStreamResponse, InnerInlongManagerClient managerClient) {
        InlongStreamInfo streamInfo = fullStreamResponse.getStreamInfo();
        this.managerClient = managerClient;
        this.streamId = streamInfo.getName();
        this.groupId = streamInfo.getInlongGroupId().substring(2);
        List<InlongStreamFieldInfo> streamFieldInfos = streamInfo.getFieldList();
        if (CollectionUtils.isNotEmpty(streamFieldInfos)) {
            this.streamFields = streamFieldInfos.stream()
                    .map(fieldInfo -> new StreamField(
                                    fieldInfo.getId(),
                                    FieldType.forName(fieldInfo.getFieldType()),
                                    fieldInfo.getFieldName(),
                                    fieldInfo.getFieldComment(),
                                    fieldInfo.getFieldValue(),
                                    fieldInfo.getIsMetaField(),
                                    fieldInfo.getFieldFormat()
                            )
                    ).collect(Collectors.toList());
        }
        List<SinkResponse> sinkList = fullStreamResponse.getSinkInfo();
        if (CollectionUtils.isNotEmpty(sinkList)) {
            this.streamSinks = sinkList.stream()
                    .map(sinkResponse -> InlongStreamSinkTransfer.parseStreamSink(sinkResponse, null))
                    .collect(Collectors.toMap(StreamSink::getSinkName, streamSink -> streamSink,
                            (sink1, sink2) -> {
                                throw new RuntimeException(
                                        String.format("duplicate sinkName:%s in stream:%s", sink1.getSinkName(),
                                                this.streamId));
                            }));
        }
        List<SourceResponse> sourceList = fullStreamResponse.getSourceInfo();
        if (CollectionUtils.isNotEmpty(sourceList)) {
            this.streamSources = sourceList.stream()
                    .map(InlongStreamSourceTransfer::parseStreamSource)
                    .collect(Collectors.toMap(StreamSource::getSourceName, streamSource -> streamSource,
                            (source1, source2) -> {
                                throw new RuntimeException(
                                        String.format("duplicate sourceName:%s in stream:%s",
                                                source1.getSourceName(), this.streamId));
                            }
                    ));
        }

    }

    public InlongStreamImpl(String group, String streamId, InnerInlongManagerClient managerClient) {
        this.managerClient = managerClient;
        this.groupId = group;
        this.streamId = streamId;
    }

    @Override
    public List<StreamField> listFields() {
        return this.streamFields;
    }

    @Override
    public Map<String, StreamSource> getSources() {
        return this.streamSources;
    }

    @Override
    public Map<String, StreamSink> getSinks() {
        return this.streamSinks;
    }

    @Override
    public Map<String, StreamTransform> getTransforms() {
        return this.streamTransforms;
    }

    @Override
    public InlongStream addSource(StreamSource source) {
        AssertUtils.notNull(source.getSourceName(), "Source name should not be empty");
        String sourceName = source.getSourceName();
        if (streamSources.get(sourceName) != null) {
            throw new IllegalArgumentException(String.format("StreamSource=%s has already be set", source));
        }
        streamSources.put(sourceName, source);
        return this;
    }

    @Override
    public InlongStream addSink(StreamSink sink) {
        AssertUtils.notNull(sink.getSinkName(), "Sink name should not be empty");
        String sinkName = sink.getSinkName();
        if (streamSinks.get(sinkName) != null) {
            throw new IllegalArgumentException(String.format("StreamSink=%s has already be set", sink));
        }
        streamSinks.put(sinkName, sink);
        return this;
    }

    @Override
    public InlongStream addTransform(StreamTransform transform) {
        AssertUtils.notNull(transform.getTransformName(), "Transform name should not be empty");
        String transformName = transform.getTransformName();
        if (streamTransforms.get(transformName) != null) {
            throw new IllegalArgumentException(String.format("TransformName=%s has already be set", transform));
        }
        streamTransforms.put(transformName, transform);
        return this;
    }

    @Override
    public InlongStream deleteSource(String sourceName) {
        streamSources.remove(sourceName);
        return this;
    }

    @Override
    public InlongStream deleteSink(String sinkName) {
        streamSinks.remove(sinkName);
        return this;
    }

    @Override
    public InlongStream deleteTransform(String transformName) {
        streamTransforms.remove(transformName);
        return this;
    }

    @Override
    public InlongStream updateSource(StreamSource source) {
        AssertUtils.notNull(source.getSourceName(), "Source name should not be empty");
        streamSources.put(source.getSourceName(), source);
        return this;
    }

    @Override
    public InlongStream updateSink(StreamSink sink) {
        AssertUtils.notNull(sink.getSinkName(), "Sink name should not be empty");
        streamSinks.put(sink.getSinkName(), sink);
        return this;
    }

    @Override
    public InlongStream updateTransform(StreamTransform transform) {
        AssertUtils.notNull(transform.getTransformName(), "Transform name should not be empty");
        streamTransforms.put(transform.getTransformName(), transform);
        return this;
    }

    @Override
    public StreamPipeline createPipeline() {
        StreamPipeline streamPipeline = new StreamPipeline();
        if (MapUtils.isEmpty(streamTransforms)) {
            StreamNodeRelationship relationship = new StreamNodeRelationship();
            relationship.setInputNodes(streamSources.keySet());
            relationship.setOutputNodes(streamSinks.keySet());
            streamPipeline.setPipeline(Lists.newArrayList(relationship));
            return streamPipeline;
        }
        Map<Set<String>, List<StreamNodeRelationship>> relationshipMap = Maps.newHashMap();
        // Create StreamNodeRelationships
        // Check preNodes
        for (StreamTransform streamTransform : streamTransforms.values()) {
            String transformName = streamTransform.getTransformName();
            Set<String> preNodes = streamTransform.getPreNodes();
            StreamNodeRelationship relationship = new StreamNodeRelationship();
            relationship.setInputNodes(preNodes);
            relationship.setOutputNodes(Sets.newHashSet(transformName));
            for (String preNode : preNodes) {
                StreamTransform transform = streamTransforms.get(preNode);
                if (transform != null) {
                    transform.addPost(transformName);
                }
            }
            relationshipMap.computeIfAbsent(preNodes, key -> Lists.newArrayList()).add(relationship);
        }
        // Check postNodes
        for (StreamTransform streamTransform : streamTransforms.values()) {
            String transformName = streamTransform.getTransformName();
            Set<String> postNodes = streamTransform.getPostNodes();
            Set<String> sinkSet = Sets.newHashSet();
            for (String postNode : postNodes) {
                StreamSink sink = streamSinks.get(postNode);
                if (sink != null) {
                    sinkSet.add(sink.getSinkName());
                }
            }
            if (CollectionUtils.isNotEmpty(sinkSet)) {
                StreamNodeRelationship relationship = new StreamNodeRelationship();
                Set<String> preNodes = Sets.newHashSet(transformName);
                relationship.setInputNodes(preNodes);
                relationship.setOutputNodes(sinkSet);
                relationshipMap.computeIfAbsent(preNodes, key -> Lists.newArrayList()).add(relationship);
            }
        }
        List<StreamNodeRelationship> relationships = Lists.newArrayList();
        // Merge StreamNodeRelationship with same preNodes
        for (Map.Entry<Set<String>, List<StreamNodeRelationship>> entry : relationshipMap.entrySet()) {
            List<StreamNodeRelationship> unmergedRelationships = entry.getValue();
            if (unmergedRelationships.size() == 1) {
                relationships.add(unmergedRelationships.get(0));
            } else {
                StreamNodeRelationship mergedRelationship = unmergedRelationships.get(0);
                for (int index = 1; index < unmergedRelationships.size(); index++) {
                    StreamNodeRelationship unmergedRelationship = unmergedRelationships.get(index);
                    unmergedRelationship.getOutputNodes().forEach(mergedRelationship::addOutputNode);
                }
                relationships.add(mergedRelationship);
            }
        }
        streamPipeline.setPipeline(relationships);
        Pair<Boolean, Pair<String, String>> circleState = streamPipeline.hasCircle();
        if (circleState.getLeft()) {
            Pair<String, String> circleNodes = circleState.getRight();
            throw new IllegalStateException(
                    String.format("There is circle dependency in streamPipeline for node=%s and node=%s",
                            circleNodes.getLeft(), circleNodes.getRight()));
        }
        return streamPipeline;
    }

    @Override
    public InlongStream update() {
        InlongStreamInfo streamInfo = new InlongStreamInfo();
        streamInfo.setInlongStreamId(streamId);
        streamInfo.setInlongGroupId(groupId);
        streamInfo = managerClient.getStreamInfo(streamInfo);
        if (streamInfo == null) {
            throw new IllegalArgumentException(
                    String.format("Stream is not exists for group=%s and stream=%s", groupId, streamId));
        }
        streamInfo.setFieldList(InlongStreamTransfer.createStreamFields(this.streamFields, streamInfo));
        StreamPipeline streamPipeline = createPipeline();
        streamInfo.setExtParams(GsonUtils.toJson(streamPipeline));
        Pair<Boolean, String> updateMsg = managerClient.updateStreamInfo(streamInfo);
        if (!updateMsg.getKey()) {
            throw new RuntimeException(String.format("Update data stream failed:%s", updateMsg.getValue()));
        }
        initOrUpdateTransform(streamInfo);
        initOrUpdateSource(streamInfo);
        initOrUpdateSink(streamInfo);
        return this;
    }

    private void initOrUpdateTransform(InlongStreamInfo streamInfo) {
        List<TransformResponse> transformResponses = managerClient.listTransform(groupId, streamId);
        List<String> updateTransformNames = Lists.newArrayList();
        for (TransformResponse transformResponse : transformResponses) {
            StreamTransform transform = InlongStreamTransformTransfer.parseStreamTransform(transformResponse);
            final String transformName = transform.getTransformName();
            final int id = transformResponse.getId();
            if (this.streamTransforms.get(transformName) == null) {
                TransformRequest transformRequest = InlongStreamTransformTransfer.createTransformRequest(transform,
                        streamInfo);
                boolean isDelete = managerClient.deleteTransform(transformRequest);
                if (!isDelete) {
                    throw new RuntimeException(String.format("Delete transform=%s failed", transformRequest));
                }
            } else {
                StreamTransform newTransform = this.streamTransforms.get(transformName);
                TransformRequest transformRequest = InlongStreamTransformTransfer.createTransformRequest(newTransform,
                        streamInfo);
                transformRequest.setId(id);
                Pair<Boolean, String> updateState = managerClient.updateTransform(transformRequest);
                if (!updateState.getKey()) {
                    throw new RuntimeException(String.format("Update transform=%s failed with err=%s", transformRequest,
                            updateState.getValue()));
                }
                updateTransformNames.add(transformName);
            }
        }
        for (Map.Entry<String, StreamTransform> transformEntry : this.streamTransforms.entrySet()) {
            String transformName = transformEntry.getKey();
            if (updateTransformNames.contains(transformName)) {
                continue;
            }
            StreamTransform transform = transformEntry.getValue();
            TransformRequest transformRequest = InlongStreamTransformTransfer.createTransformRequest(transform,
                    streamInfo);
            managerClient.createTransform(transformRequest);
        }
    }

    private void initOrUpdateSource(InlongStreamInfo streamInfo) {
        List<SourceListResponse> sourceListResponses = managerClient.listSources(groupId, streamId);
        List<String> updateSourceNames = Lists.newArrayList();
        for (SourceListResponse sourceListResponse : sourceListResponses) {
            final String sourceName = sourceListResponse.getSourceName();
            final int id = sourceListResponse.getId();
            if (this.streamSources.get(sourceName) == null) {
                boolean isDelete = managerClient.deleteSource(id);
                if (!isDelete) {
                    throw new RuntimeException(String.format("Delete source=%s failed", sourceListResponse));
                }
            } else {
                StreamSource source = this.streamSources.get(sourceName);
                SourceRequest sourceRequest = InlongStreamSourceTransfer.createSourceRequest(source, streamInfo);
                sourceRequest.setId(id);
                Pair<Boolean, String> updateState = managerClient.updateSource(sourceRequest);
                if (!updateState.getKey()) {
                    throw new RuntimeException(String.format("Update source=%s failed with err=%s", sourceRequest,
                            updateState.getValue()));
                }
                updateSourceNames.add(sourceName);
            }
        }
        for (Map.Entry<String, StreamSource> requestEntry : streamSources.entrySet()) {
            String sourceName = requestEntry.getKey();
            if (updateSourceNames.contains(sourceName)) {
                continue;
            }
            StreamSource streamSource = requestEntry.getValue();
            SourceRequest sourceRequest = InlongStreamSourceTransfer.createSourceRequest(streamSource, streamInfo);
            managerClient.createSource(sourceRequest);
        }
    }

    private void initOrUpdateSink(InlongStreamInfo streamInfo) {
        List<SinkListResponse> sinkListResponses = managerClient.listSinks(groupId, streamId);
        List<String> updateSinkNames = Lists.newArrayList();
        for (SinkListResponse sinkListResponse : sinkListResponses) {
            final String sinkName = sinkListResponse.getSinkName();
            final int id = sinkListResponse.getId();
            if (this.streamSinks.get(sinkName) == null) {
                boolean isDelete = managerClient.deleteSink(id);
                if (!isDelete) {
                    throw new RuntimeException(String.format("Delete sink=%s failed", sinkListResponse));
                }
            } else {
                StreamSink sink = this.streamSinks.get(sinkName);
                SinkRequest sinkRequest = InlongStreamSinkTransfer.createSinkRequest(sink, streamInfo);
                sinkRequest.setId(id);
                Pair<Boolean, String> updateState = managerClient.updateSink(sinkRequest);
                if (!updateState.getKey()) {
                    throw new RuntimeException(String.format("Update sink=%s failed with err=%s", sinkRequest,
                            updateState.getValue()));
                }
                updateSinkNames.add(sinkName);
            }
        }
        for (Map.Entry<String, StreamSink> requestEntry : streamSinks.entrySet()) {
            String sinkName = requestEntry.getKey();
            if (updateSinkNames.contains(sinkName)) {
                continue;
            }
            StreamSink streamSink = requestEntry.getValue();
            SinkRequest sinkRequest = InlongStreamSinkTransfer.createSinkRequest(streamSink, streamInfo);
            managerClient.createSink(sinkRequest);
        }
    }
}
