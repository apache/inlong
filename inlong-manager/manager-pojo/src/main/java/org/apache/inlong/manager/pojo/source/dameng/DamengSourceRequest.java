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

package org.apache.inlong.manager.pojo.source.dameng;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.source.SourceRequest;

/**
 * Request info of the Dameng source
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Request of the Dameng source")
@JsonTypeDefine(value = SourceType.DAMENG)
public class DamengSourceRequest extends SourceRequest {

    @ApiModelProperty("The database name")
    private String dbName;

    @ApiModelProperty("Schema name")
    private String schemaName;

    @ApiModelProperty("The table name")
    private String tableName;

    @ApiModelProperty("host")
    private String host;

    @ApiModelProperty("port")
    private String port;

    @ApiModelProperty("username")
    private String username;

    @ApiModelProperty("password")
    private String password;

    @ApiModelProperty("Scan startup mode")
    private String scanStartupMode;

    @ApiModelProperty("Primary key must be shared by all tables")
    private String primaryKey;

    @ApiModelProperty("Need transfer total database")
    @Builder.Default
    private boolean allMigration = false;

    public DamengSourceRequest() {
        this.setSourceType(SourceType.DAMENG);
    }

}
