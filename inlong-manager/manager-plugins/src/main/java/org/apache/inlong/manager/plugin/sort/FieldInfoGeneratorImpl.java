/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.manager.plugin.sort;

import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.manager.common.thirdparty.sort.FieldInfoGenerator;
import org.apache.inlong.manager.service.sort.util.FieldInfoUtils;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

@ConditionalOnProperty(name = "type", prefix = "inlong.sort.metadata.type", havingValue = "default")
@Component
public class FieldInfoGeneratorImpl implements FieldInfoGenerator {
    @Override
    public List<FieldMappingRule.FieldMappingUnit> createFieldInfo(
            List<InlongStreamFieldInfo> streamFieldList,
            List<SinkFieldResponse> fieldList,
            List<FieldInfo> sourceFields,
            List<FieldInfo> sinkFields) {
        return FieldInfoUtils.createFieldInfo(streamFieldList, fieldList, sourceFields, sinkFields);
    }

    @Override
    public List<FieldMappingRule.FieldMappingUnit> setAllMigrationFieldMapping(
            List<FieldInfo> sourceFields,
            List<FieldInfo> sinkFields) {
        return FieldInfoUtils.setAllMigrationFieldMapping(sourceFields, sinkFields);
    }
}
