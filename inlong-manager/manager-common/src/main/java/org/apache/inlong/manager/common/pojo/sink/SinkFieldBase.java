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
import lombok.Data;

/**
 * Sink field base info
 */
@Data
@ApiModel("Sink field base info")
public class SinkFieldBase {

    @ApiModelProperty("Field name")
    private String fieldName;

    @ApiModelProperty("Field type")
    private String fieldType;

    @ApiModelProperty("Field comment")
    private String fieldComment;

    @ApiModelProperty("Source field name")
    private String sourceFieldName;

    @ApiModelProperty("Source field type")
    private String sourceFieldType;

    @ApiModelProperty("Length of fixed type")
    private Integer fieldLength;

    @ApiModelProperty("Precision of decimal type, that is, field length. precision >= scale")
    private Integer fieldPrecision;

    @ApiModelProperty("Range of decimal type, that is, the number of decimal places. precision >= scale")
    private Integer fieldScale;

    @ApiModelProperty("Partition strategy, including: None, Identity, Year, Month, Day, Hour, Bucket, Truncate")
    private String partitionStrategy;

    @ApiModelProperty("Field format, including: MICROSECONDS, MILLISECONDS, SECONDS, SQL, ISO_8601"
            + " and custom such as 'yyyy-MM-dd HH:mm:ss'. This is mainly used for time format")
    private String fieldFormat;

    @ApiModelProperty("Field order")
    private Short rankNum;

}
