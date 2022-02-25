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
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.ck.ClickHouseSinkResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Stream sink service test
 */
public class ClickHouseStreamSinkServiceTest extends ServiceBaseTest {

    // Partial test data
    private static final String globalGroupId = "b_group1";
    private static final String globalStreamId = "stream1";
    private static final String globalOperator = "test_user";
    private static final String ckJdbcUrl = "jdbc:clickhouse://127.0.0.1:8123/default";
    private static final String ckUsername = "ck_user";
    private static final String ckDatabaseName = "ck_db";
    private static final String ckTableName = "ck_tbl";
    private static Integer sinkId;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    @Before
    public void saveSink() {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        ClickHouseSinkRequest sinkInfo = new ClickHouseSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(Constant.SINK_CLICKHOUSE);
        sinkInfo.setJdbcUrl(ckJdbcUrl);
        sinkInfo.setUsername(ckUsername);
        sinkInfo.setDatabaseName(ckDatabaseName);
        sinkInfo.setTableName(ckTableName);
        sinkInfo.setEnableCreateResource(Constant.DISABLE_CREATE_RESOURCE);
        sinkId = sinkService.save(sinkInfo, globalOperator);
    }

    @After
    public void deleteKafkaSink() {
        boolean result = sinkService.delete(sinkId, Constant.SINK_CLICKHOUSE, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        SinkResponse sink = sinkService.get(sinkId, Constant.SINK_CLICKHOUSE);
        Assert.assertEquals(globalGroupId, sink.getInlongGroupId());
    }

    @Test
    public void testGetAndUpdate() {
        SinkResponse response = sinkService.get(sinkId, Constant.SINK_CLICKHOUSE);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        ClickHouseSinkResponse kafkaSinkResponse = (ClickHouseSinkResponse) response;
        kafkaSinkResponse.setEnableCreateResource(Constant.ENABLE_CREATE_RESOURCE);

        ClickHouseSinkRequest request = CommonBeanUtils
                .copyProperties(kafkaSinkResponse, ClickHouseSinkRequest::new);
        boolean result = sinkService.update(request, globalOperator);
        Assert.assertTrue(result);
    }

}
