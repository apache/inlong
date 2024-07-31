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

package org.apache.inlong.manager.common.fieldtype.strategy;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.fieldtype.FieldTypeMappingReader;

import org.springframework.stereotype.Service;

/**
 * The oceanbase field type mapping strategy
 */
@Service
public class OceanBaseFieldTypeStrategy extends DefaultFieldTypeStrategy {

    public OceanBaseFieldTypeStrategy() {
        this.reader = new FieldTypeMappingReader(DataNodeType.OCEANBASE);
    }

    @Override
    public Boolean accept(String type) {
        return DataNodeType.OCEANBASE.equals(type);
    }

}
