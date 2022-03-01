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
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupConf;
import org.apache.inlong.manager.client.api.InlongGroupInfo;
import org.apache.inlong.manager.client.api.InlongStream;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.InlongStreamConf;

public class BlankInlongGroup implements InlongGroup {

    @Override
    public InlongStreamBuilder createStream(InlongStreamConf streamConf) throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }

    @Override
    public InlongGroupInfo snapshot() throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }

    @Override
    public InlongGroupInfo init() throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }

    @Override
    public InlongGroupInfo initOnUpdate(InlongGroupConf conf) throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }

    @Override
    public InlongGroupInfo suspend() throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }

    @Override
    public InlongGroupInfo restart() throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }

    @Override
    public InlongGroupInfo delete() throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }

    @Override
    public List<InlongStream> listStreams() throws Exception {
        throw new UnsupportedOperationException("Inlong group is not exists");
    }
}
