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

package org.apache.inlong.sort.base.format;

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Dynamic schema format factory
 */
public class DynamicSchemaFormatFactory {

    public static final List<AbstractDynamicSchemaFormat<?>> SUPPORT_FORMATS =
            new ArrayList<AbstractDynamicSchemaFormat<?>>() {

                private static final long serialVersionUID = 1L;

                {
                    add(CanalJsonDynamicSchemaFormat.getInstance());
                    add(DebeziumJsonDynamicSchemaFormat.getInstance());
                }
            };

    /**
     * Get format from the format name, it only supports [canal-json|debezium-json] for now
     *
     * @param identifier The identifier of this format
     * @return The dynamic format
     */
    @SuppressWarnings("rawtypes")
    public static AbstractDynamicSchemaFormat getFormat(String identifier) {
        Preconditions.checkNotNull(identifier, "The identifier is null");
        return Preconditions.checkNotNull(SUPPORT_FORMATS.stream().filter(s -> s.identifier().equals(identifier))
                .findFirst().orElse(null), "Unsupport dynamic schema format for:" + identifier);
    }

}
