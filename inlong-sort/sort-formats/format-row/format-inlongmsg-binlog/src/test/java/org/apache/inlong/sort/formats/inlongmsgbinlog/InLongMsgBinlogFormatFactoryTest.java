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
import org.apache.inlong.sort.formats.base.TableFormatDeserializer;
import org.apache.inlong.sort.formats.base.TableFormatForRowUtils;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InLongMsgBinlogFormatFactoryTest {

    private static final RowFormatInfo TEST_FORMAT_SCHEMA =
            new RowFormatInfo(
                    new String[]{"student_name", "score", "date"},
                    new FormatInfo[]{
                            StringFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            new DateFormatInfo("yyyy-MM-dd")
                    });

    @Test
    public void testCreateTableFormatDeserializer() throws Exception {
        final Map<String, String> properties =
                new InLongMsgBinlog()
                        .schema(TEST_FORMAT_SCHEMA)
                        .toProperties();
        assertNotNull(properties);

        final InLongMsgBinlogFormatDeserializer expectedDeserializer =
                new InLongMsgBinlogFormatDeserializer.Builder(TEST_FORMAT_SCHEMA)
                        .build();

        final TableFormatDeserializer actualDeserializer =
                TableFormatForRowUtils.getTableFormatDeserializer(
                        properties,
                        getClass().getClassLoader());

        assertEquals(expectedDeserializer, actualDeserializer);
    }
}