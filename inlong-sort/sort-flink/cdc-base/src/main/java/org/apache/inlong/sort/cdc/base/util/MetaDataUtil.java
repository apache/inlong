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

package org.apache.inlong.sort.cdc.base.util;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class handles the metadata of a cdc connector
 */
public class MetaDataUtil {

    private static final String FORMAT_PRECISION = "%s(%d)";
    private static final String FORMAT_PRECISION_SCALE = "%s(%d, %d)";
    private static final String REGEX_FORMATTED = "\\w.+\\([\\d ,]+\\)";

    /**
     * get a map about column name and type
     * @param tableSchema
     * @return map of field name and field type
     */
    public static Map<String, String> getType(@Nullable TableChanges.TableChange tableSchema) {
        if (tableSchema == null) {
            return null;
        }
        Map<String, String> oracleType = new LinkedHashMap<>();
        final Table table = tableSchema.getTable();
        for (Column column : table.columns()) {
            // The typeName contains precision and does not need to be formatted.
            if (column.typeName().matches(REGEX_FORMATTED)) {
                oracleType.put(column.name(), column.typeName());
                continue;
            }
            if (column.scale().isPresent()) {
                oracleType.put(
                        column.name(),
                        String.format(FORMAT_PRECISION_SCALE,
                                column.typeName(), column.length(), column.scale().get()));
            } else {
                oracleType.put(
                        column.name(),
                        String.format(FORMAT_PRECISION, column.typeName(), column.length()));
            }
        }
        return oracleType;
    }

}
