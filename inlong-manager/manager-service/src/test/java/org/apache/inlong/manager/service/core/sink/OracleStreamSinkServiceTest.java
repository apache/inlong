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

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkField;
import org.apache.inlong.manager.common.pojo.sink.StreamSink;
import org.apache.inlong.manager.common.pojo.sink.oracle.OracleSink;
import org.apache.inlong.manager.common.pojo.sink.oracle.OracleSinkRequest;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * Stream sink service test
 */
public class OracleStreamSinkServiceTest extends ServiceBaseTest {

    private static final String globalGroupId = "b_group1_oracle";
    private static final String globalStreamId = "stream_oracle";
    private static final String globalOperator = "admin";
    private static final String fieldName = "oracle_field";
    private static final String fieldType = "oracle_type";
    private static final Integer fieldId = 1;

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    /**
     * Save sink info.
     */
    public Integer saveSink(String sinkName) {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        OracleSinkRequest sinkInfo = new OracleSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.SINK_ORACLE);

        sinkInfo.setJdbcUrl("jdbc:oracle://host:port/database");
        sinkInfo.setUsername("oracle");
        sinkInfo.setPassword("inlong");
        sinkInfo.setTableName("user");
        sinkInfo.setPrimaryKey("name,age");

        sinkInfo.setSinkName(sinkName);
        sinkInfo.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        SinkField sinkField = new SinkField();
        sinkField.setFieldName(fieldName);
        sinkField.setFieldType(fieldType);
        sinkField.setId(fieldId);
        List<SinkField> sinkFieldList = new ArrayList<>();
        sinkFieldList.add(sinkField);
        sinkInfo.setSinkFieldList(sinkFieldList);
        return sinkService.save(sinkInfo, globalOperator);
    }

    /**
     * Delete oracle sink info by sink id.
     */
    public void deleteoracleSink(Integer oracleSinkId) {
        boolean result = sinkService.delete(oracleSinkId, globalOperator);
        Assertions.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        Integer oracleSinkId = this.saveSink("oracle_default1");
        StreamSink sink = sinkService.get(oracleSinkId);
        Assertions.assertEquals(globalGroupId, sink.getInlongGroupId());
        deleteoracleSink(oracleSinkId);
    }

    @Test
    public void testGetAndUpdate() {
        Integer oracleSinkId = this.saveSink("oracle_default2");
        StreamSink response = sinkService.get(oracleSinkId);
        Assertions.assertEquals(globalGroupId, response.getInlongGroupId());

        OracleSink oracleSink = (OracleSink) response;
        oracleSink.setEnableCreateResource(InlongConstants.ENABLE_CREATE_RESOURCE);

        OracleSinkRequest request = CommonBeanUtils.copyProperties(oracleSink,
                OracleSinkRequest::new);
        boolean result = sinkService.update(request, globalOperator);
        Assertions.assertTrue(result);
        deleteoracleSink(oracleSinkId);
    }

}
