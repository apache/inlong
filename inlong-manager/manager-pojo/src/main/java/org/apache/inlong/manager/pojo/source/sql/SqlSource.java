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

package org.apache.inlong.manager.pojo.source.sql;

import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Sql source info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Sql source info")
@JsonTypeDefine(value = SourceType.SQL)
public class SqlSource extends StreamSource {

    @ApiModelProperty(value = "sql", required = true)
    private String sql;

    @ApiModelProperty("Cycle unit")
    private String cycleUnit;

    @ApiModelProperty("Whether retry")
    private Boolean retry;

    @ApiModelProperty(value = "Data start time")
    private String dataTimeFrom;

    @ApiModelProperty(value = "Data end time")
    private String dataTimeTo;

    @ApiModelProperty("TimeOffset for collection, "
            + "'1m' means from one minute after, '-1m' means from one minute before, "
            + "'1h' means from one hour after, '-1h' means from one minute before, "
            + "'1d' means from one day after, '-1d' means from one minute before, "
            + "Null or blank means from current timestamp")
    private String timeOffset;

    @ApiModelProperty("Max instance count")
    private Integer maxInstanceCount;

    @ApiModelProperty("Jdbc url")
    private String jdbcUrl;

    @ApiModelProperty("Username for JDBC URL")
    private String username;

    @ApiModelProperty("jdbc password")
    private String jdbcPassword;

    @ApiModelProperty("Fetch size")
    private Integer fetchSize;

    public SqlSource() {
        this.setSourceType(SourceType.SQL);
    }

    @Override
    public SourceRequest genSourceRequest() {
        return CommonBeanUtils.copyProperties(this, SqlSourceRequest::new);
    }

}
