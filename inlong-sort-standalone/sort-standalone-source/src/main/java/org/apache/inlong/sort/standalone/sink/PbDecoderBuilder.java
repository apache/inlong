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

package org.apache.inlong.sort.standalone.sink;

import org.apache.inlong.common.pojo.sort.dataflow.SourceConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.PbConfig;
import org.apache.inlong.sdk.transform.decode.SourceDecoder;
import org.apache.inlong.sdk.transform.decode.SourceDecoderFactory;
import org.apache.inlong.sdk.transform.pojo.PbSourceInfo;

public class PbDecoderBuilder extends BaseDecoderBuilder {

    @Override
    public SourceDecoder<String> createSourceDecoder(SourceConfig sourceConfig) {
        DataTypeConfig dataTypeConfig = sourceConfig.getDataTypeConfig();
        if (dataTypeConfig instanceof PbConfig) {
            PbConfig pbConfig = (PbConfig) dataTypeConfig;
            PbSourceInfo pbSourceInfo = new PbSourceInfo(sourceConfig.getEncodingType(),
                    pbConfig.getProtoDescription(),
                    pbConfig.getRootMessageType(),
                    pbConfig.getRowsNodePath());
            return SourceDecoderFactory.createPbDecoder(pbSourceInfo);
        }
        return null;
    }
}
