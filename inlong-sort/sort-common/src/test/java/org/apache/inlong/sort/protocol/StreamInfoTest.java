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

package org.apache.inlong.sort.protocol;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.node.Node;
import org.apache.inlong.sort.protocol.node.extract.MySqlExtractNode;
import org.apache.inlong.sort.protocol.node.load.KafkaLoadNode;
import org.apache.inlong.sort.protocol.transformation.ConstantParam;
import org.apache.inlong.sort.protocol.transformation.FieldRelationShip;
import org.apache.inlong.sort.protocol.transformation.TimeUnitConstantParam;
import org.apache.inlong.sort.protocol.transformation.TimeUnitConstantParam.TimeUnit;
import org.apache.inlong.sort.protocol.transformation.WatermarkField;
import org.apache.inlong.sort.protocol.transformation.relation.NodeRelationShip;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;

public class StreamInfoTest {

    private MySqlExtractNode buildMySqlExtractNode() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()));
        WatermarkField wk = new WatermarkField(new FieldInfo("ts", new TimestampFormatInfo()),
                new ConstantParam("1"),
                new TimeUnitConstantParam(TimeUnit.MINUTE));
        return new MySqlExtractNode("1", "mysql_input", fields,
                wk, null, "id",
                Collections.singletonList("table"), "localhost", "username", "username",
                "test_database", 3306, 123, true, null);
    }

    private KafkaLoadNode buildKafkaNode() {
        List<FieldInfo> fields = Arrays.asList(new FieldInfo("id", new LongFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("salary", new FloatFormatInfo()),
                new FieldInfo("ts", new TimestampFormatInfo()));
        List<FieldRelationShip> relations = Arrays
                .asList(new FieldRelationShip(new FieldInfo("id", new LongFormatInfo()),
                                new FieldInfo("id", new LongFormatInfo())),
                        new FieldRelationShip(new FieldInfo("name", new StringFormatInfo()),
                                new FieldInfo("name", new StringFormatInfo())),
                        new FieldRelationShip(new FieldInfo("age", new IntFormatInfo()),
                                new FieldInfo("age", new IntFormatInfo())),
                        new FieldRelationShip(new FieldInfo("ts", new TimestampFormatInfo()),
                                new FieldInfo("ts", new TimestampFormatInfo()))
                );
        return new KafkaLoadNode("2", "kafka_output", fields, relations, null
                , "topic", "localhost:9092", "json",
                1, null);
    }

    private NodeRelationShip buildNodeRelation(List<Node> inputs, List<Node> outputs) {
        List<String> inputIds = inputs.stream().map(Node::getId).collect(Collectors.toList());
        List<String> outputIds = outputs.stream().map(Node::getId).collect(Collectors.toList());
        return new NodeRelationShip(inputIds, outputIds);
    }

    @Test
    public void testSerialize() throws JsonProcessingException {
        Node input = buildMySqlExtractNode();
        Node output = buildKafkaNode();
        StreamInfo streamInfo = new StreamInfo("1", Arrays.asList(input, output), Collections.singletonList(
                buildNodeRelation(Collections.singletonList(input), Collections.singletonList(output))));
        ObjectMapper objectMapper = new ObjectMapper();
        String expected = "{\"streamId\":\"1\",\"nodes\":[{\"type\":\"mysqlExtract\",\"id\":\"1\","
                + "\"name\":\"mysql_input\",\"fields\":[{\"type\":\"base\",\"name\":\"id\","
                + "\"formatInfo\":{\"type\":\"long\"}},{\"type\":\"base\",\"name\":\"name\","
                + "\"formatInfo\":{\"type\":\"string\"}},{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}},{\"type\":\"base\",\"name\":\"salary\","
                + "\"formatInfo\":{\"type\":\"float\"}},{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}}],"
                + "\"watermarkField\":{\"type\":\"watermark\",\"timeAttr\":{\"type\":\"base\","
                + "\"name\":\"ts\",\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}},"
                + "\"interval\":{\"type\":\"constant\",\"value\":\"1\"},\"timeUnit\":{\"type\":\"timeUnitConstant\","
                + "\"timeUnit\":\"MINUTE\",\"value\":\"MINUTE\"}},\"primaryKey\":\"id\",\"tableNames\":[\"table\"],"
                + "\"hostname\":\"localhost\",\"username\":\"username\",\"password\":\"username\","
                + "\"database\":\"test_database\",\"port\":3306,\"serverId\":123,\"incrementalSnapshotEnabled\":true},"
                + "{\"type\":\"kafkaLoad\",\"id\":\"2\",\"name\":\"kafka_output\",\"fields\":[{\"type\":\"base\","
                + "\"name\":\"id\",\"formatInfo\":{\"type\":\"long\"}},{\"type\":\"base\",\"name\":\"name\","
                + "\"formatInfo\":{\"type\":\"string\"}},{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}},{\"type\":\"base\",\"name\":\"salary\","
                + "\"formatInfo\":{\"type\":\"float\"}},{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}}],"
                + "\"fieldRelationShips\":[{\"type\":\"fieldRelationShip\",\"inputField\":{\"type\":\"base\","
                + "\"name\":\"id\",\"formatInfo\":{\"type\":\"long\"}},\"outputField\":{\"type\":\"base\","
                + "\"name\":\"id\",\"formatInfo\":{\"type\":\"long\"}}},{\"type\":\"fieldRelationShip\","
                + "\"inputField\":{\"type\":\"base\",\"name\":\"name\",\"formatInfo\":{\"type\":\"string\"}},"
                + "\"outputField\":{\"type\":\"base\",\"name\":\"name\",\"formatInfo\":{\"type\":\"string\"}}},"
                + "{\"type\":\"fieldRelationShip\",\"inputField\":{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}},\"outputField\":{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}}},{\"type\":\"fieldRelationShip\","
                + "\"inputField\":{\"type\":\"base\",\"name\":\"ts\",\"formatInfo\":{\"type\":\"timestamp\","
                + "\"format\":\"yyyy-MM-dd HH:mm:ss\"}},\"outputField\":{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}}}],"
                + "\"topic\":\"topic\",\"bootstrapServers\":\"localhost:9092\",\"format\":\"json\","
                + "\"sinkParallelism\":1}],\"relations\":[{\"type\":\"baseRelation\",\"inputs\":[\"1\"],"
                + "\"outputs\":[\"2\"]}]}";
        assertEquals(expected, objectMapper.writeValueAsString(streamInfo));
    }

    @Test
    public void testDeserialize() throws JsonProcessingException {
        Node input = buildMySqlExtractNode();
        Node output = buildKafkaNode();
        StreamInfo streamInfo = new StreamInfo("1", Arrays.asList(input, output), Collections.singletonList(
                buildNodeRelation(Collections.singletonList(input), Collections.singletonList(output))));
        ObjectMapper objectMapper = new ObjectMapper();
        String streamInfoStr = "{\"streamId\":\"1\",\"nodes\":[{\"type\":\"mysqlExtract\",\"id\":\"1\","
                + "\"name\":\"mysql_input\",\"fields\":[{\"type\":\"base\",\"name\":\"id\","
                + "\"formatInfo\":{\"type\":\"long\"}},{\"type\":\"base\",\"name\":\"name\","
                + "\"formatInfo\":{\"type\":\"string\"}},{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}},{\"type\":\"base\",\"name\":\"salary\","
                + "\"formatInfo\":{\"type\":\"float\"}},{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}}],"
                + "\"watermarkField\":{\"type\":\"watermark\",\"timeAttr\":{\"type\":\"base\","
                + "\"name\":\"ts\",\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}},"
                + "\"interval\":{\"type\":\"constant\",\"value\":\"1\"},\"timeUnit\":{\"type\":\"timeUnitConstant\","
                + "\"timeUnit\":\"MINUTE\",\"value\":\"MINUTE\"}},\"primaryKey\":\"id\",\"tableNames\":[\"table\"],"
                + "\"hostname\":\"localhost\",\"username\":\"username\",\"password\":\"username\","
                + "\"database\":\"test_database\",\"port\":3306,\"serverId\":123,\"incrementalSnapshotEnabled\":true},"
                + "{\"type\":\"kafkaLoad\",\"id\":\"2\",\"name\":\"kafka_output\",\"fields\":[{\"type\":\"base\","
                + "\"name\":\"id\",\"formatInfo\":{\"type\":\"long\"}},{\"type\":\"base\",\"name\":\"name\","
                + "\"formatInfo\":{\"type\":\"string\"}},{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}},{\"type\":\"base\",\"name\":\"salary\","
                + "\"formatInfo\":{\"type\":\"float\"}},{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}}],"
                + "\"fieldRelationShips\":[{\"type\":\"fieldRelationShip\",\"inputField\":{\"type\":\"base\","
                + "\"name\":\"id\",\"formatInfo\":{\"type\":\"long\"}},\"outputField\":{\"type\":\"base\","
                + "\"name\":\"id\",\"formatInfo\":{\"type\":\"long\"}}},{\"type\":\"fieldRelationShip\","
                + "\"inputField\":{\"type\":\"base\",\"name\":\"name\",\"formatInfo\":{\"type\":\"string\"}},"
                + "\"outputField\":{\"type\":\"base\",\"name\":\"name\",\"formatInfo\":{\"type\":\"string\"}}},"
                + "{\"type\":\"fieldRelationShip\",\"inputField\":{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}},\"outputField\":{\"type\":\"base\",\"name\":\"age\","
                + "\"formatInfo\":{\"type\":\"int\"}}},{\"type\":\"fieldRelationShip\","
                + "\"inputField\":{\"type\":\"base\",\"name\":\"ts\",\"formatInfo\":{\"type\":\"timestamp\","
                + "\"format\":\"yyyy-MM-dd HH:mm:ss\"}},\"outputField\":{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}}}],"
                + "\"topic\":\"topic\",\"bootstrapServers\":\"localhost:9092\",\"format\":\"json\","
                + "\"sinkParallelism\":1}],\"relations\":[{\"type\":\"baseRelation\",\"inputs\":[\"1\"],"
                + "\"outputs\":[\"2\"]}]}";
        StreamInfo expected = objectMapper.readValue(streamInfoStr, StreamInfo.class);
        assertEquals(expected, streamInfo);
    }
}
