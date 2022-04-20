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

package org.apache.inlong.manager.common.thirdparty.sort;

import org.apache.inlong.manager.common.pojo.sink.SinkFieldResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamFieldInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.transformation.FieldMappingRule.FieldMappingUnit;

import java.util.List;

/**
 * Interface of field info generator.
 * Use to generate user-defined field info.
 */
public interface FieldInfoGenerator {

    /**
     * Method to create field info.
     *
     * @param streamFieldList streamFieldList
     * @param fieldList fieldList
     * @param sourceFields sourceFields
     * @param sinkFields sinkFields
     * @return List of {@link FieldMappingUnit}
     */
    List<FieldMappingUnit> createFieldInfo(
            List<InlongStreamFieldInfo> streamFieldList,
            List<SinkFieldResponse> fieldList,
            List<FieldInfo> sourceFields,
            List<FieldInfo> sinkFields);

    /**
     * Method to set all migration field mapping.
     *
     * @param sourceFields sourceFields
     * @param sinkFields sinkFields
     * @return List of {@link FieldMappingUnit}
     */
    List<FieldMappingUnit> setAllMigrationFieldMapping(
            List<FieldInfo> sourceFields,
            List<FieldInfo> sinkFields);
}
