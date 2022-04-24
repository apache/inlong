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

package org.apache.inlong.manager.service.sort.util;

import com.google.common.collect.Lists;
import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.pojo.transform.TransformDefinition;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import java.util.List;

public class FilterFunctionUtils {

    public static List<FilterFunction> createFilterFunctions(TransformDefinition transformDefinition) {
        TransformType transformType = transformDefinition.getTransformType();
        switch (transformType) {
            case DE_DUPLICATION:
                return Lists.newArrayList();
            case FILTER:

            case SPLITTER:
            case JOINER:
            case STRING_REPLACER:
                return Lists.newArrayList();
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported transformType=%s for Inlong", transformType));
        }
    }

}
