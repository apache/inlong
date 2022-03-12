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

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.inlong.sort.protocol.FieldInfo;

public class FieldMappingRule implements TransformationRule, Serializable {

    private static final long serialVersionUID = -3285027125865704371L;

    @JsonProperty("field_mapping_units")
    private final FieldMappingUnit[] fieldMappingUnits;

    @JsonCreator
    public FieldMappingRule(
            @JsonProperty("field_mapping_units") FieldMappingUnit[] fieldMappingUnits) {
        this.fieldMappingUnits = fieldMappingUnits;
    }

    @JsonProperty("field_mapping_units")
    public FieldMappingUnit[] getFieldMappingUnits() {
        return fieldMappingUnits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldMappingRule that = (FieldMappingRule) o;
        return Arrays.equals(fieldMappingUnits, that.fieldMappingUnits);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldMappingUnits);
    }

    public static class FieldMappingUnit implements Serializable {

        private static final long serialVersionUID = 1525401917124693789L;

        @JsonProperty("source_field")
        private final FieldInfo sourceFieldInfo;

        @JsonProperty("sink_field")
        private final FieldInfo sinkFieldInfo;

        @JsonCreator
        public FieldMappingUnit(
                @JsonProperty("source_field") FieldInfo sourceFieldInfo,
                @JsonProperty("sink_field") FieldInfo sinkFieldInfo) {
            this.sourceFieldInfo = Preconditions.checkNotNull(sourceFieldInfo);
            this.sinkFieldInfo = Preconditions.checkNotNull(sinkFieldInfo);
        }

        @JsonProperty("source_field")
        public FieldInfo getSourceFieldInfo() {
            return sourceFieldInfo;
        }

        @JsonProperty("sink_field")
        public FieldInfo getSinkFieldInfo() {
            return sinkFieldInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldMappingUnit that = (FieldMappingUnit) o;
            return Objects.equals(sourceFieldInfo, that.sourceFieldInfo) && Objects.equals(sinkFieldInfo,
                    that.sinkFieldInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceFieldInfo, sinkFieldInfo);
        }
    }
}
