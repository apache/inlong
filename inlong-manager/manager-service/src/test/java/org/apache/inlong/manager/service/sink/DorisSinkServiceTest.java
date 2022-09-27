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

package org.apache.inlong.manager.service.sink;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.pojo.sink.SinkRequest;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.sink.doris.DorisSink;
import org.apache.inlong.manager.pojo.sink.doris.DorisSinkRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Doris sink service test
 */
public class DorisSinkServiceTest extends ServiceBaseTest {

    // Partial test data
    private final String globalGroupId = "b_group1";
    private final String globalStreamId = "stream1_doris";
    private final String globalOperator = "admin";
    private final String dorisJdbcUrl = "jdbc:mysql://127.0.0.1:6603/default";
    private final String dorisUsername = "doris_user";
    private final String dorisPassword = "doris_passwd";
    private final String dorisTableName = "doris_tbl";
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save sink info.
     */
    public Integer saveSink(String sinkName) {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        DorisSinkRequest sinkRequest = new DorisSinkRequest();
        sinkRequest.setInlongGroupId(globalGroupId);
        sinkRequest.setInlongStreamId(globalStreamId);
        sinkRequest.setSinkName(sinkName);
        sinkRequest.setSinkType(SinkType.DORIS);
        sinkRequest.setJdbcUrl(dorisJdbcUrl);
        sinkRequest.setUsername(dorisUsername);
        sinkRequest.setPassword(dorisPassword);
        sinkRequest.setTableName(dorisTableName);
        sinkRequest.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        return sinkService.save(sinkRequest, globalOperator);
    }

    /**
     * Delete sink by sink id.
     */
    public void deleteSink(Integer sinkId) {
        boolean result = sinkService.delete(sinkId, globalOperator);
        Assertions.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer sinkId = this.saveSink("default1");
        StreamSink sink = sinkService.get(sinkId);
        Assertions.assertEquals(globalGroupId, sink.getInlongGroupId());
        deleteSink(sinkId);
    }

    @Test
    public void testGetAndUpdate() {
        Integer sinkId = this.saveSink("default2");
        StreamSink streamSink = sinkService.get(sinkId);
        Assertions.assertEquals(globalGroupId, streamSink.getInlongGroupId());

        DorisSink sink = (DorisSink) streamSink;
        sink.setEnableCreateResource(InlongConstants.ENABLE_CREATE_RESOURCE);
        SinkRequest request = sink.genSinkRequest();
        boolean result = sinkService.update(request, globalOperator);
        Assertions.assertTrue(result);

        deleteSink(sinkId);
    }

}
