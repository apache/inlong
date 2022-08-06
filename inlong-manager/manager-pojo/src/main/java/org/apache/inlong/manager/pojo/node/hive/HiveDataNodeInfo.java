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

package org.apache.inlong.manager.pojo.node.hive;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeInfo;

import javax.validation.constraints.NotBlank;

/**
 * Data node info for hive
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.HIVE)
@ApiModel("Data node info for hive")
public class HiveDataNodeInfo extends DataNodeInfo {

    @ApiModelProperty("Hive JDBC URL, such as jdbc:hive2://${ip}:${port}")
    private String jdbcUrl;

    @ApiModelProperty("Username of the Hive server")
    private String username;

    @ApiModelProperty("User password of the Hive server")
    private String password;

    @NotBlank(message = "dbName cannot be blank")
    @ApiModelProperty("Target database name")
    private String dbName;

    @NotBlank(message = "tableName cannot be blank")
    @ApiModelProperty("Target table name")
    private String tableName;

    @NotBlank(message = "dataPath cannot be blank")
    @ApiModelProperty("Data path, such as: hdfs://ip:port/user/hive/warehouse/test.db")
    private String dataPath;

    @ApiModelProperty("Version for Hive, such as: 3.2.1")
    private String hiveVersion;

    @ApiModelProperty("Config directory of Hive on HDFS, needed by sort in light mode, must include hive-site.xml")
    private String hiveConfDir;

    public HiveDataNodeInfo() {
        this.setType(DataNodeType.HIVE);
    }

    @Override
    public HiveDataNodeRequest genRequest() {
        return CommonBeanUtils.copyProperties(this, HiveDataNodeRequest::new);
    }
}
