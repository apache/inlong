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

package org.apache.inlong.manager.service.datatype;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.DataTypeConfig;
import org.apache.inlong.common.pojo.sort.dataflow.dataType.PbConfig;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage.FieldInfo;
import org.apache.inlong.manager.pojo.sink.SinkField;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PbDataTypeOperator implements DataTypeOperator {

    @Override
    public boolean accept(DataTypeEnum type) {
        return DataTypeEnum.PB.equals(type);
    }

    @Override
    public List<FieldInfo> parseFields(String str, InlongStreamInfo streamInfo) throws Exception {
        return new ArrayList<>();
    }

    @Override
    public DataTypeConfig getDataTypeConfig(InlongStreamInfo streamInfo) {
        return new PbConfig();
    }

    @Override
    public Map<String, Object> parseTransform(InlongStreamInfo streamInfo, List<SinkField> fieldList,
            String transformSql,
            String data) {
        return new ConcurrentHashMap<>();
    }
}
