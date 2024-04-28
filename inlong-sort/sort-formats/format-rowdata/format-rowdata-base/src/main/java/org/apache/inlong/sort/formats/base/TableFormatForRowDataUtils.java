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

package org.apache.inlong.sort.formats.base;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;

import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class TableFormatForRowDataUtils extends TableFormatUtils {

    public static RowFormatInfo projectRowFormatInfo(RowFormatInfo rowFormatInfo,
            DataType projectedDataType) {
        List<String> projectedFieldNames = DataType.getFieldNames(projectedDataType);
        Map<String, FormatInfo> rowFormatInfoMap = new HashMap<>();
        IntStream.range(0, rowFormatInfo.getFieldNames().length)
                .forEach(i -> rowFormatInfoMap.put(
                        rowFormatInfo.getFieldNames()[i], rowFormatInfo.getFieldFormatInfos()[i]));

        String[] projectedFieldNameArray = new String[projectedFieldNames.size()];
        FormatInfo[] projectedFieldFormatInfoArray = new FormatInfo[projectedFieldNames.size()];

        // keep the fields order same with the projectedFieldNames
        IntStream.range(0, projectedFieldNames.size())
                .forEach(i -> {
                    String projectedFieldName = projectedFieldNames.get(i);
                    FormatInfo projectedFormatInfo = rowFormatInfoMap.get(projectedFieldName);
                    if (projectedFormatInfo == null) {
                        throw new IllegalStateException(
                                "Could not find the format info of field[" + projectedFieldName + "]. " +
                                        "The projected fields is " + projectedFieldNames + ", the row format " +
                                        "info is " + rowFormatInfo);
                    }
                    projectedFieldNameArray[i] = projectedFieldName;
                    projectedFieldFormatInfoArray[i] = projectedFormatInfo;
                });

        return new RowFormatInfo(projectedFieldNameArray, projectedFieldFormatInfoArray);
    }
}
