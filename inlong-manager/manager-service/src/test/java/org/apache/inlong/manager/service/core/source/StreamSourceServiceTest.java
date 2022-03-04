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

package org.apache.inlong.manager.service.core.source;

import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceRequest;
import org.apache.inlong.manager.common.pojo.source.binlog.BinlogSourceResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.source.StreamSourceService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Stream source service test
 */
public class StreamSourceServiceTest extends ServiceBaseTest {

    private final String globalGroupId = "b_group1";
    private final String globalStreamId = "stream1";
    private final String globalOperator = "test_user";
    private final String sourceName = "default";

    @Autowired
    private StreamSourceService sourceService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    public Integer saveSource() {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);

        BinlogSourceRequest sourceInfo = new BinlogSourceRequest();
        sourceInfo.setInlongGroupId(globalGroupId);
        sourceInfo.setInlongStreamId(globalStreamId);
        sourceInfo.setSourceName(sourceName);
        sourceInfo.setSourceType(Constant.SOURCE_BINLOG);
        return sourceService.save(sourceInfo, globalOperator);
    }

    @Test
    public void testSaveAndDelete() {
        Integer id = this.saveSource();
        Assert.assertNotNull(id);

        boolean result = sourceService.delete(id, Constant.SOURCE_BINLOG, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer id = this.saveSource();

        SourceResponse source = sourceService.get(id, Constant.SOURCE_BINLOG);
        Assert.assertEquals(globalGroupId, source.getInlongGroupId());

        sourceService.delete(id, Constant.SOURCE_BINLOG, globalOperator);
    }

    @Test
    public void testGetAndUpdate() {
        Integer id = this.saveSource();
        SourceResponse response = sourceService.get(id, Constant.SOURCE_BINLOG);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        BinlogSourceResponse binlogResponse = (BinlogSourceResponse) response;

        BinlogSourceRequest request = CommonBeanUtils.copyProperties(binlogResponse, BinlogSourceRequest::new);
        boolean result = sourceService.update(request, globalOperator);
        Assert.assertTrue(result);

        sourceService.delete(id, Constant.SOURCE_BINLOG, globalOperator);
    }

}
