/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.singletenant.flink.utils;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.apache.inlong.sort.formats.base.TableFormatUtils;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;

public class CommonUtils {

    public static TableSchema getTableSchema(SinkInfo sinkInfo) {
        TableSchema.Builder builder = new Builder();
        FieldInfo[] fieldInfos = sinkInfo.getFields();

        for (FieldInfo fieldInfo : fieldInfos) {
            builder.field(
                    fieldInfo.getName(),
                    TableFormatUtils.getType(fieldInfo.getFormatInfo().getTypeInfo()));
        }

        return builder.build();
    }
}
