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

package org.apache.inlong.manager.service.stream;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.dao.entity.InlongStreamFieldEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkFieldEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.sink.AddFieldRequest;
import org.apache.inlong.manager.pojo.sink.ParseFieldRequest;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.sink.hive.HiveSinkRequest;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

import static org.apache.inlong.manager.common.consts.InlongConstants.STATEMENT_TYPE_JSON;
import static org.apache.inlong.manager.common.consts.InlongConstants.STATEMENT_TYPE_SQL;

public class InlongStreamTest extends ServiceBaseTest {

    private final String globalGroupId = "default_group";
    private final String globalStreamId = "default_stream";
    private final String globalOperator = "admin";
    private final String sinkName = "default_sink";
    private final String jdbcUrl = "127.0.0.1:8080";

    @Autowired
    private InlongStreamService streamService;
    @Autowired
    protected StreamSinkService streamSinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;
    @Autowired
    private InlongStreamFieldEntityMapper streamFieldEntityMapper;
    @Autowired
    private StreamSinkFieldEntityMapper streamSinkFieldEntityMapper;

    @Test
    public void testParseStreamFieldsByJson() {
        String streamFieldsJson =
                "[{\"name\":\"name0\",\"type\":\"string\",\"desc\":\"desc0\"},{\"name\":\"name1\",\"type\":\"string\"}]";
        List<StreamField> expectStreamFields = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            StreamField field = new StreamField();
            field.setFieldName("name" + i);
            field.setFieldType("string");
            if (i == 0) {
                field.setFieldComment("desc0");
            }
            expectStreamFields.add(field);
        }
        StreamField[] expectResult = expectStreamFields.toArray(new StreamField[0]);
        ParseFieldRequest request =
                ParseFieldRequest.builder().method(STATEMENT_TYPE_JSON).statement(streamFieldsJson).build();
        List<StreamField> streamFields = streamService.parseFields(request);
        StreamField[] result = streamFields.toArray(new StreamField[0]);
        Assertions.assertArrayEquals(expectResult, result);
    }

    @Test
    public void testParseSinkFieldsByJson() {
        String sinkFieldsJson =
                "[{\"name\":\"sinkFieldName0\",\"type\":\"string\",\"desc\":\"desc0 content\"},{\"name\":\"sinkFieldName1\",\"type\":\"string\"}]";
        List<SinkField> expectSinkFields = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            SinkField field = new SinkField();
            field.setFieldName("sinkFieldName" + i);
            field.setFieldType("string");
            if (i == 0) {
                field.setFieldComment("desc0 content");
            }
            expectSinkFields.add(field);
        }
        SinkField[] expectResult = expectSinkFields.toArray(new SinkField[0]);
        ParseFieldRequest parseFieldRequest =
                ParseFieldRequest.builder().method(STATEMENT_TYPE_JSON).statement(sinkFieldsJson).build();
        List<SinkField> sinkFields = streamSinkService.parseFields(parseFieldRequest);
        SinkField[] result = sinkFields.toArray(new SinkField[0]);
        Assertions.assertArrayEquals(expectResult, result);
    }

    @Test
    public void testParseStreamFieldsBySql() {
        String streamFieldsSql = "CREATE TABLE my_table (name0 VARCHAR(50) comment 'desc0 content', name1 VARCHAR(50))";
        List<StreamField> expectStreamFields = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            StreamField field = new StreamField();
            field.setFieldName("name" + i);
            field.setFieldType("string");
            if (i == 0) {
                field.setFieldComment("desc0 content");
            }
            expectStreamFields.add(field);
        }
        StreamField[] expectResult = expectStreamFields.toArray(new StreamField[0]);
        ParseFieldRequest request =
                ParseFieldRequest.builder().method(STATEMENT_TYPE_SQL).statement(streamFieldsSql).build();
        List<StreamField> streamFields = streamService.parseFields(request);
        StreamField[] result = streamFields.toArray(new StreamField[0]);
        Assertions.assertArrayEquals(expectResult, result);
    }

    @Test
    public void testParseSinkFieldsBySql() {
        String sinkFieldsSql =
                "CREATE TABLE my_table (sinkFieldName0 VARCHAR(50) comment 'desc0 content', sinkFieldName1 VARCHAR(50))";
        List<SinkField> expectSinkFields = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            SinkField field = new SinkField();
            field.setFieldName("sinkFieldName" + i);
            field.setFieldType("varchar");
            if (i == 0) {
                field.setFieldComment("desc0 content");
            }
            expectSinkFields.add(field);
        }
        SinkField[] expectResult = expectSinkFields.toArray(new SinkField[0]);
        ParseFieldRequest parseFieldRequest =
                ParseFieldRequest.builder().method(STATEMENT_TYPE_SQL).statement(sinkFieldsSql).build();
        List<SinkField> sinkFields = streamSinkService.parseFields(parseFieldRequest);
        SinkField[] result = sinkFields.toArray(new SinkField[0]);
        Assertions.assertArrayEquals(expectResult, result);
    }

    @Test
    public void testAddFieldsForStream() {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        HiveSinkRequest sinkInfo = new HiveSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.HIVE);
        sinkInfo.setEnableCreateResource(InlongConstants.DISABLE_CREATE_RESOURCE);
        sinkInfo.setSinkName(sinkName);
        sinkInfo.setJdbcUrl(jdbcUrl);
        Integer id = streamSinkService.save(sinkInfo, globalOperator);
        AddFieldRequest addFieldRequest = new AddFieldRequest();
        addFieldRequest.setInlongGroupId(globalGroupId);
        addFieldRequest.setInlongStreamId(globalStreamId);
        List<SinkField> sinkFields = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            SinkField field = new SinkField();
            field.setInlongGroupId(globalGroupId);
            field.setInlongStreamId(globalStreamId);
            field.setSourceFieldName("sinkFieldName" + i);
            field.setSourceFieldType("string");
            field.setFieldName("sinkFieldName" + i);
            field.setFieldType("string");
            if (i == 0) {
                field.setFieldComment("desc0 content");
            }
            sinkFields.add(field);
        }
        addFieldRequest.setSinkFieldList(sinkFields);
        streamService.addFields(addFieldRequest);
        for (int i = 1; i < 5; i++) {
            SinkField field = new SinkField();
            field.setInlongGroupId(globalGroupId);
            field.setInlongStreamId(globalStreamId);
            field.setSourceFieldName("sinkFieldName" + i);
            field.setSourceFieldType("string");
            field.setFieldName("sinkFieldName" + i);
            field.setFieldType("string");
            sinkFields.add(field);
        }
        streamService.addFields(addFieldRequest);
        List<InlongStreamFieldEntity> streamFieldEntityList =
                streamFieldEntityMapper.selectByIdentifier(globalGroupId, globalStreamId);
        List<StreamSinkFieldEntity> sinkFieldEntityList = streamSinkFieldEntityMapper.selectBySinkId(id);
        Assertions.assertEquals(5, streamFieldEntityList.size());
        Assertions.assertEquals(5, sinkFieldEntityList.size());

    }

}
