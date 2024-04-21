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

package org.apache.inlong.manager.pojo.source.paimon;

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

import java.util.HashMap;
import java.util.List;

/**
 * The Paimon source info
 */
@Data
@SuperBuilder
@AllArgsConstructor
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Paimon source info")
@JsonTypeDefine(value = SourceType.PAIMON)
public class PaimonSource extends StreamSource {

    @ApiModelProperty("The database name of Paimon")
    private String dbName;

    @ApiModelProperty("The table name of the Paimon")
    private String tableName;

    @ApiModelProperty("The catalog uri of the Paimon")
    private String catalogUri;

    @ApiModelProperty("The dfs base path of the Paimon")
    private String warehouse;

    @ApiModelProperty("The check file interval in minutes")
    private int checkIntervalInMinus;

    @ApiModelProperty("The flag indicate whether skip files in compaction")
    private boolean readStreamingSkipCompaction;

    @ApiModelProperty("The start commit id")
    private String readStartCommit;

    @ApiModelProperty("Extended properties")
    private List<HashMap<String, String>> extList;

    public PaimonSource() {
        this.setSourceType(SourceType.PAIMON);
    }

    @Override
    public SourceRequest genSourceRequest() {
        return CommonBeanUtils.copyProperties(this, PaimonSourceRequest::new);
    }

}
