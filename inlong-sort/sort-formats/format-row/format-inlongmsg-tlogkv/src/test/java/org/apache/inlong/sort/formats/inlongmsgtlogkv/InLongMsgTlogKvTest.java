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

package org.apache.inlong.sort.formats.inlongmsgtlogkv;

import org.apache.inlong.sort.formats.inlongmsg.InLongMsgValidator;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link InLongMsgTlogKv} descriptor.
 */
public class InLongMsgTlogKvTest extends DescriptorTestBase {

    private static final String TEST_SCHEMA =
            "{" +
                    "\"type\":\"row\"," +
                    "\"fieldFormats\":[{" +
                    "\"name\":\"student_name\"," +
                    "\"format\":{\"type\":\"string\"}" +
                    "},{" +
                    "\"name\":\"score\"," +
                    "\"format\":{\"type\":\"int\"}" +
                    "},{" +
                    "\"name\":\"date\"," +
                    "\"format\":{" +
                    "\"type\":\"date\"," +
                    "\"format\":\"yyyy-MM-dd\"" +
                    "}" +
                    "}]" +
                    "}";

    private static final Descriptor CUSTOM_DESCRIPTOR_WITH_SCHEMA =
            new InLongMsgTlogKv()
                    .schema(TEST_SCHEMA)
                    .timeFieldName("time")
                    .attributesFieldName("attributes")
                    .delimiter(';')
                    .entryDelimiter('&')
                    .kvDelimiter('=')
                    .charset(StandardCharsets.UTF_8)
                    .escapeCharacter('\\')
                    .quoteCharacter('\"')
                    .nullLiteral("n/a")
                    .ignoreErrors();

    @Test(expected = ValidationException.class)
    public void testInvalidIgnoreParseErrors() {
        addPropertyAndVerify(CUSTOM_DESCRIPTOR_WITH_SCHEMA, "format.escape-character", "DDD");
    }

    @Test(expected = ValidationException.class)
    public void testMissingSchema() {
        removePropertyAndVerify(CUSTOM_DESCRIPTOR_WITH_SCHEMA, "format.schema");
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public List<Descriptor> descriptors() {
        return Collections.singletonList(CUSTOM_DESCRIPTOR_WITH_SCHEMA);
    }

    @Override
    public List<Map<String, String>> properties() {
        final Map<String, String> props1 = new HashMap<>();
        props1.put("format.type", "inlong-msg-tlogkv");
        props1.put("format.property-version", "1");
        props1.put("format.schema", TEST_SCHEMA);
        props1.put("format.time-field-name", "time");
        props1.put("format.attributes-field-name", "attributes");
        props1.put("format.delimiter", ";");
        props1.put("format.entry-delimiter", "&");
        props1.put("format.kv-delimiter", "=");
        props1.put("format.charset", "UTF-8");
        props1.put("format.escape-character", "\\");
        props1.put("format.quote-character", "\"");
        props1.put("format.null-literal", "n/a");
        props1.put("format.ignore-errors", "true");

        return Collections.singletonList(props1);
    }

    @Override
    public DescriptorValidator validator() {
        return new InLongMsgValidator();
    }
}
