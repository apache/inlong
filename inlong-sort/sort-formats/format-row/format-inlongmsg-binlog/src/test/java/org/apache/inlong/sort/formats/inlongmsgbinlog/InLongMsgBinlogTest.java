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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatUtils.marshall;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.DEFAULT_METADATA_FIELD_NAME;

public class InLongMsgBinlogTest extends DescriptorTestBase {

    private static final RowFormatInfo TEST_SCHEMA =
            new RowFormatInfo(
                    new String[]{"student_name", "score", "date"},
                    new FormatInfo[]{
                            StringFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            new DateFormatInfo("yyyy-MM-dd")
                    });

    private static final Descriptor CUSTOM_DESCRIPTOR_WITH_SCHEMA =
            new InLongMsgBinlog()
                    .schema(TEST_SCHEMA)
                    .timeFieldName("time")
                    .attributesFieldName("attributes")
                    .metadataFieldName(DEFAULT_METADATA_FIELD_NAME)
                    .ignoreErrors();

    @Test(expected = ValidationException.class)
    public void testInvalidIgnoreParseErrors() {
        addPropertyAndVerify(CUSTOM_DESCRIPTOR_WITH_SCHEMA, "format.ignore-errors", "DDD");
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
        props1.put("format.type", "inlong-msg-binlog");
        props1.put("format.property-version", "1");
        props1.put("format.schema", marshall(TEST_SCHEMA));
        props1.put("format.time-field-name", "time");
        props1.put("format.attributes-field-name", "attributes");
        props1.put("format.metadata-field-name", DEFAULT_METADATA_FIELD_NAME);
        props1.put("format.ignore-errors", "true");

        return Collections.singletonList(props1);
    }

    @Override
    public DescriptorValidator validator() {
        return new InLongMsgBinlogValidator();
    }
}
