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
import java.util.List;
import org.apache.commons.collections.MapUtils;
import org.apache.inlong.manager.client.api.StreamSource;
import org.apache.inlong.manager.client.api.StreamSink;
import org.apache.inlong.manager.client.api.DataStream;
import org.apache.inlong.manager.client.api.DataStreamBuilder;
import org.apache.inlong.manager.client.api.DataStreamConf;
import org.apache.inlong.manager.client.api.StreamField;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.api.inner.InnerStreamContext;
import org.apache.inlong.manager.client.api.util.DataStreamTransfer;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamFieldInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;

public class DefaultDataStreamBuilder extends DataStreamBuilder {

    private DataStreamImpl dataStream;

    private DataStreamConf streamConf;

    private InnerGroupContext groupContext;

    private InnerStreamContext streamContext;

    private InnerInlongManagerClient managerClient;

    public DefaultDataStreamBuilder(
            DataStreamConf streamConf,
            InnerGroupContext groupContext,
            InnerInlongManagerClient managerClient) {
        this.streamConf = streamConf;
        this.groupContext = groupContext;
        this.managerClient = managerClient;
        if (MapUtils.isEmpty(groupContext.getStreamContextMap())) {
            groupContext.setStreamContextMap(Maps.newHashMap());
        }
        DataStreamInfo streamInfo = DataStreamTransfer.createDataStreamInfo(streamConf, groupContext.getBusinessInfo());
        InnerStreamContext streamContext = new InnerStreamContext(streamInfo);
        groupContext.setStreamContext(streamContext);
        this.streamContext = streamContext;
        this.dataStream = new DataStreamImpl();
    }

    @Override
    public DataStreamBuilder source(StreamSource source) {
        //todo create SourceRequest
        return this;
    }

    @Override
    public DataStreamBuilder sink(StreamSink storage) {
        dataStream.setStreamSink(storage);
        StorageRequest storageRequest = DataStreamTransfer.createStorageRequest(storage,
                streamContext.getDataStreamInfo());
        streamContext.setStorageRequest(storageRequest);
        return this;
    }

    @Override
    public DataStreamBuilder fields(List<StreamField> fieldList) {
        dataStream.setStreamFields(fieldList);
        List<DataStreamFieldInfo> streamFieldInfos = DataStreamTransfer.createStreamFields(fieldList,
                streamContext.getDataStreamInfo());
        streamContext.updateStreamFields(streamFieldInfos);
        return this;
    }

    @Override
    public DataStream init() {
        DataStreamInfo dataStreamInfo = streamContext.getDataStreamInfo();
        String streamIndex = managerClient.createStreamInfo(dataStreamInfo);
        dataStreamInfo.setId(Integer.parseInt(streamIndex));
        //todo save source

        StorageRequest storageRequest = streamContext.getStorageRequest();
        String storageIndex = managerClient.createStorage(storageRequest);
        storageRequest.setId(Integer.parseInt(storageIndex));
        return dataStream;
    }
}
