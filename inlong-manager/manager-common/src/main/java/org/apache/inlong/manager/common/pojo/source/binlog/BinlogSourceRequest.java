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

package org.apache.inlong.manager.common.pojo.source.binlog;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.util.JsonTypeDefine;

/**
 * Request of the binlog source info
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Request of the binlog source info")
@JsonTypeDefine(value = Constant.SOURCE_DB_BINLOG)
public class BinlogSourceRequest extends SourceRequest {

    public BinlogSourceRequest(){
        this.setSourceType(SourceType.DB_BINLOG.toString());
    }

    @ApiModelProperty("Source database name")
    private String dbName;

    @ApiModelProperty("Source table name")
    private String tableName;

    @ApiModelProperty("Data charset")
    private String charset;

    @ApiModelProperty(value = "Table fields, separated by commas")
    private String tableFields;

    @ApiModelProperty(value = "Data separator, default is 0x01")
    private String dataSeparator = "0x01";

    @ApiModelProperty(value = "Whether to skip delete events in binlog, default: 1, that is skip")
    private Integer skipDelete;

    @ApiModelProperty(value = "Collect starts from the specified binlog location, and it is modified after delivery."
            + "If it is empty, an empty string is returned")
    private String startPosition;

    @ApiModelProperty(value = "When the field value is null, the replaced field defaults to 'null'")
    private String nullFieldChar;

}
