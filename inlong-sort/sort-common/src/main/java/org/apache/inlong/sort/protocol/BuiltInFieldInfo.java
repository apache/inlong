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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.formats.common.FormatInfo;

/**
 * built-in field info.
 */
public class BuiltInFieldInfo extends FieldInfo {

    public enum BuiltInField {
        DATA_TIME,
        MYSQL_METADATA_DATABASE,
        MYSQL_METADATA_TABLE,
        MYSQL_METADATA_EVENT_TIME,
        MYSQL_METADATA_IS_DDL,
        MYSQL_METADATA_EVENT_TYPE
    }

    private static final long serialVersionUID = -3436204467879205139L;

    @JsonProperty("builtin_field")
    private final BuiltInField builtInField;

    @JsonCreator
    public BuiltInFieldInfo(
            @JsonProperty("name") String name,
            @JsonProperty("format_info") FormatInfo formatInfo,
            @JsonProperty("builtin_field") BuiltInField builtInField) {
        super(name, formatInfo);
        this.builtInField = builtInField;
    }

    public BuiltInField getBuiltInField() {
        return builtInField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BuiltInFieldInfo that = (BuiltInFieldInfo) o;
        return builtInField == that.builtInField
                && super.equals(that);
    }
}
