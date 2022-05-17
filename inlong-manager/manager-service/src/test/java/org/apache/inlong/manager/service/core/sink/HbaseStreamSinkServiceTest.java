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
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.hbase.HbaseSinkResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class HbaseStreamSinkServiceTest extends ServiceBaseTest {

    private static final String globalGroupId = "b_group1";
    private static final String globalStreamId = "stream1_hbase";
    private static final String globalOperator = "admin";
    private static final String tableName = "table1";
    private static final String nameSpace = "space1";
    private static final String rowkey = "rowkey1";
    private static final String zookeeperQuorum = "127.0.0.1:9092";

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save sink info.
     */
    public Integer saveSink(String sinkName) {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);

        HbaseSinkRequest sinkInfo = new HbaseSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.SINK_HBASE);
        sinkInfo.setEnableCreateResource(GlobalConstants.DISABLE_CREATE_RESOURCE);
        sinkInfo.setSinkName(sinkName);
        sinkInfo.setTableName(tableName);
        sinkInfo.setNameSpace(nameSpace);
        sinkInfo.setRowKey(rowkey);
        sinkInfo.setZookeeperQuorum(zookeeperQuorum);
        return sinkService.save(sinkInfo, globalOperator);
    }

    /**
     * Delete Hbase sink info by sink id.
     */
    public void deleteHbaseSink(Integer hbaseSinkId) {
        boolean result = sinkService.delete(hbaseSinkId, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer hbaseSinkId = this.saveSink("default1");
        SinkResponse sink = sinkService.get(hbaseSinkId);
        Assert.assertEquals(globalGroupId, sink.getInlongGroupId());
        deleteHbaseSink(hbaseSinkId);
    }

    @Test
    public void testGetAndUpdate() {
        Integer hbaseSinkId = this.saveSink("default2");
        SinkResponse response = sinkService.get(hbaseSinkId);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        HbaseSinkResponse hbaseSinkResponse = (HbaseSinkResponse) response;
        hbaseSinkResponse.setEnableCreateResource(GlobalConstants.ENABLE_CREATE_RESOURCE);

        HbaseSinkRequest request = CommonBeanUtils.copyProperties(hbaseSinkResponse, HbaseSinkRequest::new);
        boolean result = sinkService.update(request, globalOperator);
        Assert.assertTrue(result);
        deleteHbaseSink(hbaseSinkId);
    }

}
