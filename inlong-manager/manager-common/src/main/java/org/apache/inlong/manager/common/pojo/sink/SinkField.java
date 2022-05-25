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

package org.apache.inlong.manager.common.pojo.sink;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Sink field info.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Sink field configuration")
public class SinkField {

    @ApiModelProperty("Field index")
    private Integer id;

    @ApiModelProperty(value = "inlong group id", required = true)
    private String inlongGroupId;

    @ApiModelProperty(value = "inlong stream id", required = true)
    private String inlongStreamId;

    @ApiModelProperty(value = "Field name", required = true)
    private String fieldName;

    @ApiModelProperty(value = "Field type", required = true)
    private String fieldType;

    @ApiModelProperty(value = "Field comment")
    private String fieldComment;

    @ApiModelProperty(value = "Field value for constants")
    private String fieldValue;

    @ApiModelProperty(value = "Is predefined field, 1: yes, 0: no")
    private Integer isPredefinedField;

    @ApiModelProperty(value = "Value expression of predefined field")
    private String preExpression;

    @ApiModelProperty("Is this field a meta field, 0: no, 1: yes")
    private Integer isMetaField = 0;

    @ApiModelProperty("Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, SQL, ISO_8601"
            + " and custom such as 'yyyy-MM-dd HH:mm:ss'. This is mainly used for time format")
    private String fieldFormat;

    @ApiModelProperty("Origin node name which stream fields belong")
    private String originNodeName;

    @ApiModelProperty("Origin field name before transform operation")
    private String originFieldName;

    @ApiModelProperty("Extra Param in JSON style")
    private String extParams;

    @ApiModelProperty("Source field name")
    private String sourceFieldName;

    @ApiModelProperty("Source field type")
    private String sourceFieldType;

    public SinkField(int index, String fieldType, String fieldName, String sourceFieldType, String sourceFieldName) {
        this(index, fieldType, fieldName, null, null, sourceFieldName, sourceFieldType, 0, null);
    }

    public SinkField(int index, String fieldType, String fieldName, String fieldComment,
            String fieldValue, String sourceFieldName, String sourceFieldType,
            Integer isMetaField, String fieldFormat) {
        this(index, fieldType, fieldName, fieldComment, fieldValue, isMetaField, fieldFormat);
        this.sourceFieldName = sourceFieldName;
        this.sourceFieldType = sourceFieldType;
    }

    public SinkField(int index, String fieldType, String fieldName, String fieldComment, String fieldValue,
            Integer isMetaField, String originNodeName) {
        this(fieldType, index, fieldName, fieldComment, fieldValue);
        this.isMetaField = isMetaField;
        this.originNodeName = originNodeName;
    }

    public SinkField(String fieldType, int index, String fieldName, String fieldComment, String fieldValue) {
        this.id = index;
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.fieldComment = fieldComment;
        this.fieldValue = fieldValue;
    }
}
