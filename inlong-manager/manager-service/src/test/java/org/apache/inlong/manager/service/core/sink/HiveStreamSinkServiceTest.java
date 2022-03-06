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

package org.apache.inlong.manager.service.core.sink;

import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hive.HiveSinkResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Stream sink service test
 */
public class HiveStreamSinkServiceTest extends ServiceBaseTest {

    private final String globalGroupId = "b_group1";
    private final String globalStreamId = "stream1";
    private final String globalOperator = "admin";
    private final String sinkName = "default";

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    public Integer saveSink() {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);

        HiveSinkRequest sinkInfo = new HiveSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(Constant.SINK_HIVE);
        sinkInfo.setEnableCreateResource(Constant.DISABLE_CREATE_RESOURCE);
        sinkInfo.setSinkName(sinkName);
        return sinkService.save(sinkInfo, globalOperator);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveSink();
        Assert.assertNotNull(id);

        boolean result = sinkService.delete(id, Constant.SINK_HIVE, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer id = this.saveSink();

        SinkResponse sink = sinkService.get(id, Constant.SINK_HIVE);
        Assert.assertEquals(globalGroupId, sink.getInlongGroupId());

        sinkService.delete(id, Constant.SINK_HIVE, globalOperator);
    }

    @Test
    public void testGetAndUpdate() {
        Integer id = this.saveSink();
        SinkResponse response = sinkService.get(id, Constant.SINK_HIVE);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        HiveSinkResponse hiveResponse = (HiveSinkResponse) response;
        hiveResponse.setEnableCreateResource(Constant.DISABLE_CREATE_RESOURCE);

        HiveSinkRequest request = CommonBeanUtils.copyProperties(hiveResponse, HiveSinkRequest::new);
        boolean result = sinkService.update(request, globalOperator);
        Assert.assertTrue(result);
    }

}
