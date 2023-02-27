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

import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

public class InlongStreamTest extends ServiceBaseTest {

    @Autowired
    protected StreamSinkService streamSinkService;

    @Test
    public void testParseStreamFields() {
        String streamFieldsJson = "{\"name0\":\"string\",\"name1\":\"string\"}";
        List<StreamField> expectStreamFields = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            StreamField field = new StreamField();
            field.setFieldName("name" + i);
            field.setFieldType("string");
            expectStreamFields.add(field);
        }
        StreamField[] expectResult = expectStreamFields.toArray(new StreamField[0]);
        List<StreamField> streamFields = streamService.parseFields(streamFieldsJson);
        StreamField[] result = streamFields.toArray(new StreamField[0]);
        Assertions.assertArrayEquals(expectResult, result);
    }

    @Test
    public void testParseSinkFields() {
        String sinkFieldsJson = "{\"sinkFieldName0\":\"string\",\"sinkFieldName1\":\"string\"}";
        List<SinkField> expectSinkFields = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            SinkField field = new SinkField();
            field.setFieldName("sinkFieldName" + i);
            field.setFieldType("string");
            expectSinkFields.add(field);
        }
        SinkField[] expectResult = expectSinkFields.toArray(new SinkField[0]);
        List<SinkField> sinkFields = streamSinkService.parseFields(sinkFieldsJson);
        SinkField[] result = sinkFields.toArray(new SinkField[0]);
        Assertions.assertArrayEquals(expectResult, result);
    }
}
