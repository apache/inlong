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

package org.apache.inlong.manager.common.util;

import org.apache.inlong.manager.common.enums.TransformType;
import org.apache.inlong.manager.common.pojo.transform.TransformDefinition;
import org.apache.inlong.manager.common.pojo.transform.deduplication.DeDuplicationDefinition;
import org.apache.inlong.manager.common.pojo.transform.filter.FilterDefinition;
import org.apache.inlong.manager.common.pojo.transform.joiner.JoinerDefinition;
import org.apache.inlong.manager.common.pojo.transform.replacer.StringReplacerDefinition;
import org.apache.inlong.manager.common.pojo.transform.splitter.SplitterDefinition;

public class TransformDefinitionUtils {

    public static TransformDefinition parseTransformDefinition(String transformDefinition,
            TransformType transformType) {
        switch (transformType) {
            case FILTER:
                return JsonUtils.parse(transformDefinition, FilterDefinition.class);
            case JOINER:
                return JsonUtils.parse(transformDefinition, JoinerDefinition.class);
            case SPLITTER:
                return JsonUtils.parse(transformDefinition, SplitterDefinition.class);
            case DE_DUPLICATION:
                return JsonUtils.parse(transformDefinition, DeDuplicationDefinition.class);
            case STRING_REPLACER:
                return JsonUtils.parse(transformDefinition, StringReplacerDefinition.class);
            default:
                throw new IllegalArgumentException(String.format("Unsupported transformType for %s", transformType));
        }
    }

}
