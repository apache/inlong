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

package org.apache.inlong.manager.client.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.inlong.manager.common.enums.FieldType;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@ApiModel("Sink field configuration")
public class SinkField extends StreamField {

    @ApiModelProperty("Source field name")
    private String sourceFieldName;

    @ApiModelProperty("Source field type")
    private FieldType sourceFieldType;

    public SinkField(int index, FieldType fieldType, String fieldName, FieldType sourceFieldType,
            String sourceFieldName) {
        this(index, fieldType, fieldName, null, null, sourceFieldName, sourceFieldType, 0, null);
    }

    public SinkField(int index, FieldType fieldType, String fieldName, String fieldComment,
            String fieldValue, String sourceFieldName, FieldType sourceFieldType,
            Integer isMetaField, String fieldFormat) {
        super(index, fieldType, fieldName, fieldComment, fieldValue, isMetaField, fieldFormat);
        this.sourceFieldName = sourceFieldName;
        this.sourceFieldType = sourceFieldType;
    }
}
