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

package org.apache.inlong.sort.protocol.transformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.TimeUnitConstantParam.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WaterMarkFieldTest {

    @Test
    public void testSerialize() throws JsonProcessingException {
        WatermarkField waterMarkField = new WatermarkField(new FieldInfo("ts", new TimestampFormatInfo()),
                new ConstantParam("10"), new TimeUnitConstantParam(TimeUnit.HOUR));
        ObjectMapper objectMapper = new ObjectMapper();
        String expected = "{\"type\":\"watermark\",\"timeAttr\":{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}},"
                + "\"interval\":{\"type\":\"constant\",\"value\":\"10\"},"
                + "\"timeUnit\":{\"type\":\"timeUnitConstant\",\"timeUnit\":\"HOUR\",\"value\":\"HOUR\"}}";
        assertEquals(expected, objectMapper.writeValueAsString(waterMarkField));
    }

    @Test
    public void testDeserialize() throws JsonProcessingException {
        WatermarkField waterMarkField = new WatermarkField(new FieldInfo("ts", new TimestampFormatInfo()),
                new ConstantParam("10"), new TimeUnitConstantParam(TimeUnit.HOUR));
        ObjectMapper objectMapper = new ObjectMapper();
        String str = "{\"type\":\"watermark\",\"timeAttr\":{\"type\":\"base\",\"name\":\"ts\","
                + "\"formatInfo\":{\"type\":\"timestamp\",\"format\":\"yyyy-MM-dd HH:mm:ss\"}},"
                + "\"interval\":{\"type\":\"constant\",\"value\":\"10\"},\"timeUnit\":{\"type\":"
                + "\"timeUnitConstant\"," + "\"timeUnit\":\"HOUR\",\"value\":\"HOUR\"}}";
        WatermarkField expected = objectMapper.readValue(str, WatermarkField.class);
        assertEquals(expected, waterMarkField);
    }

}
