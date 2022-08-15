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
import org.apache.inlong.manager.pojo.sink.hudi.HudiSink;
import org.apache.inlong.manager.pojo.sink.hudi.HudiSinkRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *  Hudi sink service test.
 */
public class HudiSinkServiceTest extends ServiceBaseTest {

    private final String globalGroupId = "b_group1_hudi";
    private final String globalStreamId = "stream1_hudi";
    private final String globalOperator = "admin";

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save sink info.
     */
    public Integer saveSink(String sinkName) {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        HudiSinkRequest sinkInfo = new HudiSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.HUDI);

        sinkInfo.setPath("/tmp/inlong/hudi");
        sinkInfo.setTableType("COPY_ON_WRITE");
        sinkInfo.setTableName("user");
        sinkInfo.setPrimaryKey("name,age");
        sinkInfo.setSinkName(sinkName);
        sinkInfo.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        return sinkService.save(sinkInfo, globalOperator);
    }

    /**
     * Delete sink info by sink id.
     */
    public void deleteSink(Integer sinkId) {
        boolean result = sinkService.delete(sinkId, globalOperator);
        Assertions.assertTrue(result);
    }

    @Test
    public void testSaveAndDelete() {
        Integer sinkId = this.saveSink("default1");
        Assertions.assertNotNull(sinkId);
        boolean result = sinkService.delete(sinkId, globalOperator);
        Assertions.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer sinkId = this.saveSink("default2");
        StreamSink sink = sinkService.get(sinkId);
        Assertions.assertEquals(globalGroupId, sink.getInlongGroupId());
        sinkService.delete(sinkId, globalOperator);
    }

    @Test
    public void testGetAndUpdate() {
        Integer sinkId = this.saveSink("default3");
        StreamSink streamSink = sinkService.get(sinkId);
        Assertions.assertEquals(globalGroupId, streamSink.getInlongGroupId());

        HudiSink sink = (HudiSink) streamSink;
        sink.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        SinkRequest request = sink.genSinkRequest();
        boolean result = sinkService.update(request, globalOperator);
        Assertions.assertTrue(result);

        sinkService.delete(sinkId, globalOperator);
    }
}
