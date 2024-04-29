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

package org.apache.inlong.sdk.transform.pojo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * TestTransformConfig
 * 
 */
public class TestTransformConfig {

    @Test
    public void testCsv() {
        try {
            FieldInfo ftime = new FieldInfo();
            ftime.setName("ftime");
            List<FieldInfo> fields = new ArrayList<>();
            fields.add(ftime);
            SourceInfo csvSource = new CsvSourceInfo("UTF-8", "|", "\\", fields);
            SinkInfo csvSink = new CsvSinkInfo("UTF-8", "|", "\\", fields);
            String transformSql = "select ftime from source";
            TransformConfig config = new TransformConfig(csvSource, csvSink, transformSql);
            ObjectMapper objectMapper = new ObjectMapper();
            String configString = objectMapper.writeValueAsString(config);
            System.out.println(configString);
            Assert.assertEquals(configString, "{\"sourceInfo\":{\"type\":\"csv\",\"charset\":\"UTF-8\","
                    + "\"delimiter\":\"|\",\"escapeChar\":\"\\\\\",\"fields\":[{\"name\":\"ftime\"}]},"
                    + "\"sinkInfo\":{\"type\":\"csv\",\"charset\":\"UTF-8\","
                    + "\"delimiter\":\"|\",\"escapeChar\":\"\\\\\",\"fields\":[{\"name\":\"ftime\"}]},"
                    + "\"transformSql\":\"select ftime from source\"}");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKv() {
        try {
            FieldInfo ftime = new FieldInfo();
            ftime.setName("ftime");
            List<FieldInfo> fields = new ArrayList<>();
            fields.add(ftime);
            SourceInfo kvSource = new KvSourceInfo("UTF-8", fields);
            SinkInfo kvSink = new KvSinkInfo("UTF-8", fields);
            String transformSql = "select ftime from source";
            TransformConfig config = new TransformConfig(kvSource, kvSink, transformSql);
            ObjectMapper objectMapper = new ObjectMapper();
            String configString = objectMapper.writeValueAsString(config);
            System.out.println(configString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPb() {
        try {
            FieldInfo ftime = new FieldInfo();
            ftime.setName("ftime");
            List<FieldInfo> fields = new ArrayList<>();
            fields.add(ftime);
            SourceInfo pbSource = new PbSourceInfo("UTF-8", "syntax = \"proto3\";", "root");
            SinkInfo csvSink = new CsvSinkInfo("UTF-8", "|", "\\", fields);
            String transformSql = "select ftime from source";
            TransformConfig config = new TransformConfig(pbSource, csvSink, transformSql);
            ObjectMapper objectMapper = new ObjectMapper();
            String configString = objectMapper.writeValueAsString(config);
            System.out.println(configString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJson() {
        try {
            FieldInfo ftime = new FieldInfo();
            ftime.setName("ftime");
            List<FieldInfo> fields = new ArrayList<>();
            fields.add(ftime);
            SourceInfo jsonSource = new JsonSourceInfo("UTF-8", "root");
            SinkInfo csvSink = new CsvSinkInfo("UTF-8", "|", "\\", fields);
            String transformSql = "select ftime from source";
            TransformConfig config = new TransformConfig(jsonSource, csvSink, transformSql);
            ObjectMapper objectMapper = new ObjectMapper();
            String configString = objectMapper.writeValueAsString(config);
            System.out.println(configString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
