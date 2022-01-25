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

import com.google.common.base.Preconditions;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class FieldInfo {
    @JsonProperty("name")
    private final String name;

    @JsonProperty("format_info")
    private final FormatInfo formatInfo;

    @JsonCreator
    public FieldInfo(
            @JsonProperty("name") String name,
            @JsonProperty("format_info") FormatInfo formatInfo) {
        this.name = Preconditions.checkNotNull(name);
        this.formatInfo = Preconditions.checkNotNull(formatInfo);
    }

    public String getName() {
        return name;
    }

    public FormatInfo getFormatInfo() {
        return formatInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldInfo fieldInfo = (FieldInfo) o;
        return name.equals(fieldInfo.name) && formatInfo.equals(fieldInfo.formatInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, formatInfo);
    }
}
