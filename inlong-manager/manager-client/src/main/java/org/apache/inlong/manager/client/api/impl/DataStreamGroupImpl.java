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

import java.util.List;
import org.apache.inlong.manager.client.api.DataStream;
import org.apache.inlong.manager.client.api.DataStreamBuilder;
import org.apache.inlong.manager.client.api.DataStreamConf;
import org.apache.inlong.manager.client.api.DataStreamGroup;
import org.apache.inlong.manager.client.api.DataStreamGroupConf;
import org.apache.inlong.manager.client.api.DataStreamGroupInfo;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;

public class DataStreamGroupImpl implements DataStreamGroup {

    private DataStreamGroupConf conf;

    private InlongClientImpl inlongClient;

    private InnerInlongManagerClient managerClient;

    private InnerGroupContext groupContext;

    public DataStreamGroupImpl(DataStreamGroupConf conf, InlongClientImpl inlongClient) {
        this.conf = conf;
        this.inlongClient = inlongClient;
        this.groupContext = new InnerGroupContext();
        BusinessInfo businessInfo = DataStreamGroupTransfer.createBusinessInfo(conf);
        this.groupContext.setBusinessInfo(businessInfo);
        if (this.managerClient == null) {
            this.managerClient = new InnerInlongManagerClient(inlongClient);
        }
    }

    @Override
    public DataStreamBuilder createDataStream(DataStreamConf dataStreamConf) throws Exception {
        return null;
    }

    @Override
    public DataStreamGroupInfo init() throws Exception {
        return null;
    }

    @Override
    public DataStreamGroupInfo suspend() throws Exception {
        return null;
    }

    @Override
    public DataStreamGroupInfo restart() throws Exception {
        return null;
    }

    @Override
    public DataStreamGroupInfo delete() throws Exception {
        return null;
    }

    @Override
    public List<DataStream> listStreams(String streamGroupId) throws Exception {
        return null;
    }
}
