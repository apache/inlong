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

package org.apache.inlong.manager.common.pojo.datastream;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;

import java.util.List;

/**
 * All response info on the data stream page, including data stream, data source, and data storage
 */
@Data
@ApiModel("All response info on the data stream page")
public class FullStreamResponse {

    @ApiModelProperty("Data stream information")
    private DataStreamInfo streamInfo;

    @ApiModelProperty("Basic information of file data source")
    private SourceFileBasicInfo fileBasicInfo;

    @ApiModelProperty("File data source details")
    private List<SourceFileDetailInfo> fileDetailInfoList;

    @ApiModelProperty("DB data source basic information")
    private SourceDbBasicInfo dbBasicInfo;

    @ApiModelProperty("DB data source details")
    private List<SourceDbDetailInfo> dbDetailInfoList;

    @ApiModelProperty("Data storage information")
    private List<StorageResponse> storageInfo;

}
