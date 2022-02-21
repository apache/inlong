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

package org.apache.inlong.manager.common.pojo.stream;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceDbBasicInfo;
import org.apache.inlong.manager.common.pojo.source.SourceDbDetailInfo;
import org.apache.inlong.manager.common.pojo.source.SourceFileBasicInfo;
import org.apache.inlong.manager.common.pojo.source.SourceFileDetailInfo;

import java.util.List;

/**
 * All response info on the inlong stream page, including inlong stream, source, and stream sink
 */
@Data
@ApiModel("All response info on the inlong stream page")
public class FullStreamResponse {

    @ApiModelProperty("Inlong stream information")
    private InlongStreamInfo streamInfo;

    @ApiModelProperty("Basic information of file source")
    private SourceFileBasicInfo fileBasicInfo;

    @ApiModelProperty("File source details")
    private List<SourceFileDetailInfo> fileDetailInfoList;

    @ApiModelProperty("DB source basic information")
    private SourceDbBasicInfo dbBasicInfo;

    @ApiModelProperty("DB source details")
    private List<SourceDbDetailInfo> dbDetailInfoList;

    @ApiModelProperty("Data sink information")
    private List<SinkResponse> sinkInfo;

}
