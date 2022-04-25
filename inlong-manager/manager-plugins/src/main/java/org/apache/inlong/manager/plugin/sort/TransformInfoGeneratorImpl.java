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

import org.apache.inlong.manager.common.thirdparty.sort.TransformInfoGenerator;
import org.apache.inlong.manager.service.sort.util.TransformInfoUtils;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule;
import org.apache.inlong.sort.protocol.transformation.TransformationInfo;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@ConditionalOnProperty(name = "type", prefix = "inlong.sort.transinfo", havingValue = "default")
@Component
public class TransformInfoGeneratorImpl implements TransformInfoGenerator {
    @Override
    public TransformationInfo createTransformationInfo(FieldMappingRule fieldMappingRule, Map<String, Object> properties) {
        return TransformInfoUtils.createTransformationInfo(fieldMappingRule);
    }
}
