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

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.client.api.util.InlongStreamSinkTransfer;
import org.apache.inlong.manager.client.api.util.InlongStreamSourceTransfer;
import org.apache.inlong.manager.common.enums.FieldType;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class InlongStreamImpl extends InlongStream {

    private String name;

    private Map<String, StreamSource> streamSources = Maps.newHashMap();

    private Map<String, StreamSink> streamSinks = Maps.newHashMap();

    private List<StreamField> streamFields = Lists.newArrayList();

    public InlongStreamImpl(FullStreamResponse fullStreamResponse) {
        InlongStreamInfo streamInfo = fullStreamResponse.getStreamInfo();
        this.name = streamInfo.getName();
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
                                                this.name));
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
                                                source1.getSourceName(), this.name));
                            }
                    ));
        }

    }

    public InlongStreamImpl(String name) {
        this.name = name;
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
    public void addSource(StreamSource source) {
        AssertUtil.notNull(source.getSourceName(), "Source name should not be empty");
        String sourceName = source.getSourceName();
        if (streamSources.get(sourceName) != null) {
            throw new IllegalArgumentException(String.format("StreamSource=%s has already be set", source));
        }
        streamSources.put(sourceName, source);
    }

    @Override
    public void addSink(StreamSink sink) {
        AssertUtil.notNull(sink.getSinkName(), "Sink name should not be empty");
        String sinkName = sink.getSinkName();
        if (streamSinks.get(sinkName) != null) {
            throw new IllegalArgumentException(String.format("StreamSink=%s has already be set", sink));
        }
        streamSinks.put(sinkName, sink);
    }

    @Override
    public void updateSource(StreamSource source) {
        AssertUtil.notNull(source.getSourceName(), "Source name should not be empty");
        streamSources.put(source.getSourceName(), source);
    }

    @Override
    public void updateSink(StreamSink sink) {
        AssertUtil.notNull(sink.getSinkName(), "Sink name should not be empty");
        streamSinks.put(sink.getSinkName(), sink);
    }

}
