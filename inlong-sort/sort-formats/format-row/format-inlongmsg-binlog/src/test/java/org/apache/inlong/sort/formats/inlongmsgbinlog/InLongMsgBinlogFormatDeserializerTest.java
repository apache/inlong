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

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.DEFAULT_METADATA_FIELD_NAME;
import static org.junit.Assert.assertEquals;

public class InLongMsgBinlogFormatDeserializerTest {

    private static final RowFormatInfo TEST_ROW_INFO =
            new RowFormatInfo(
                    new String[]{"f1", "f2", "f3", "f4", "f5", "f6"},
                    new FormatInfo[]{
                            IntFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            IntFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE,
                            StringFormatInfo.INSTANCE
                    });

    @Test
    public void testRowType() {

        InLongMsgBinlogFormatDeserializer deserializer =
                new InLongMsgBinlogFormatDeserializer.Builder(TEST_ROW_INFO).build();

        TypeInformation<Row> expectedRowType =
                Types.ROW_NAMED(
                        new String[]{
                                DEFAULT_TIME_FIELD_NAME,
                                DEFAULT_ATTRIBUTES_FIELD_NAME,
                                DEFAULT_METADATA_FIELD_NAME,
                                "f1",
                                "f2",
                                "f3",
                                "f4",
                                "f5",
                                "f6"
                        },
                        Types.SQL_TIMESTAMP,
                        Types.MAP(Types.STRING, Types.STRING),
                        Types.MAP(Types.STRING, Types.STRING),
                        Types.INT,
                        Types.INT,
                        Types.INT,
                        Types.STRING,
                        Types.STRING,
                        Types.STRING);

        assertEquals(expectedRowType, deserializer.getProducedType());
    }
}
