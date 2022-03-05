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
import java.util.Locale;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("Stream field configuration")
public class StreamField {

    @ApiModelProperty("Field index")
    private int index;

    public enum FieldType {
        INT,
        LONG,
        DOUBLE,
        FLOAT,
        STRING,
        CHAR,
        BYTE,
        BINARY,
        BOOLEAN,
        DATE,
        TIMESTAMP;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static FieldType forName(String name) {
            for (FieldType value : values()) {
                if (value.toString().equals(name)) {
                    return value;
                }
            }
            throw new IllegalArgumentException(String.format("Unsupported FieldType : %s", name));
        }
    }

    @ApiModelProperty(value = "Field type", required = true)
    private FieldType fieldType;

    @ApiModelProperty(value = "Field name", required = true)
    private String fieldName;

    @ApiModelProperty(value = "Field comment")
    private String fieldComment;

    @ApiModelProperty(value = "Field value for constants")
    private String fieldValue;
}
