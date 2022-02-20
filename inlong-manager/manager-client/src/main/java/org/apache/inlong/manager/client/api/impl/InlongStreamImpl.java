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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.StreamField.FieldType;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.client.api.util.InlongStreamTransfer;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

import java.util.List;
import java.util.stream.Collectors;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
public class InlongStreamImpl extends InlongStream {

    private String name;

    private StreamSource streamSource;

    private StreamSink streamSink;

    private List<StreamField> streamFields;

    public InlongStreamImpl(FullStreamResponse fullStreamResponse) {
        InlongStreamInfo streamInfo = fullStreamResponse.getStreamInfo();
        this.name = streamInfo.getName();
        List<InlongStreamFieldInfo> streamFieldInfos = streamInfo.getFieldList();
        this.streamFields = streamFieldInfos.stream().map(streamFieldInfo -> {
            return new StreamField(streamFieldInfo.getId(),
                    FieldType.forName(streamFieldInfo.getFieldType()),
                    streamFieldInfo.getFieldName(),
                    streamFieldInfo.getFieldComment(),
                    streamFieldInfo.getFieldValue()
            );
        }).collect(Collectors.toList());
        List<SinkResponse> sinkList = fullStreamResponse.getSinkInfo();
        if (CollectionUtils.isNotEmpty(sinkList)) {
            this.streamSink = InlongStreamTransfer.parseStreamSink(sinkList.get(0));
        }
        // todo generate source
    }

    public InlongStreamImpl(String name) {
        this.name = name;
    }

    @Override
    public List<StreamField> listFields() {
        return this.streamFields;
    }

    @Override
    public StreamSource getSource() {
        return this.streamSource;
    }

    @Override
    public StreamSink getSink() {
        return this.streamSink;
    }

}
