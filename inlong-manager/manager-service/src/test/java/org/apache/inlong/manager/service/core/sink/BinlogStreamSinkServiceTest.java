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

import org.apache.inlong.manager.common.enums.GlobalConstants;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.mysql.BinlogSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.mysql.BinlogSinkResponse;
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
public class BinlogStreamSinkServiceTest extends ServiceBaseTest {

    private static final String globalGroupId = "b_group1_binlog";
    private static final String globalStreamId = "stream1_binlog";
    private static final String globalOperator = "admin";

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save sink info.
     */
    public Integer saveSink(String sinkName) {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId,
                globalOperator);
        BinlogSinkRequest sinkInfo = new BinlogSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.SINK_BINLLOG);

        sinkInfo.setJdbcUrl("jdbc:mysql://localhost:5432/database");
        sinkInfo.setUsername("binlog");
        sinkInfo.setPassword("inlong");
        sinkInfo.setTableName("user");
        sinkInfo.setPrimaryKey("name,age");

        sinkInfo.setSinkName(sinkName);
        sinkInfo.setEnableCreateResource(GlobalConstants.DISABLE_CREATE_RESOURCE);
        return sinkService.save(sinkInfo, globalOperator);
    }

    /**
     * Delete binlog sink info by sink id.
     */
    public void deleteBinlogSink(Integer binlogSinkId) {
        boolean result = sinkService.delete(binlogSinkId, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer binlogSinkId = this.saveSink("binlog_default1");
        SinkResponse sink = sinkService.get(binlogSinkId);
        Assert.assertEquals(globalGroupId, sink.getInlongGroupId());
        deleteBinlogSink(binlogSinkId);
    }

    @Test
    public void testGetAndUpdate() {
        Integer binlogSinkId = this.saveSink("binlog_default2");
        SinkResponse response = sinkService.get(binlogSinkId);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        BinlogSinkResponse binlogSinkResponse = (BinlogSinkResponse) response;
        binlogSinkResponse.setEnableCreateResource(GlobalConstants.ENABLE_CREATE_RESOURCE);

        BinlogSinkRequest request = CommonBeanUtils.copyProperties(binlogSinkResponse,
                BinlogSinkRequest::new);
        boolean result = sinkService.update(request, globalOperator);
        Assert.assertTrue(result);
        deleteBinlogSink(binlogSinkId);
    }

}
