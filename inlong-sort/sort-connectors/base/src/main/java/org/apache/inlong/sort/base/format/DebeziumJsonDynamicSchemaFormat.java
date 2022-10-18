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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import static org.apache.inlong.sort.base.Constants.AFTER;
import static org.apache.inlong.sort.base.Constants.BEFORE;
import static org.apache.inlong.sort.base.Constants.PK_NAMES;
import static org.apache.inlong.sort.base.Constants.SOURCE;

/**
 * Debezium json dynamic format
 */
public class DebeziumJsonDynamicSchemaFormat extends JsonDynamicSchemaFormat {

    private static final String IDENTIFIER = "debezium-json";

    private static final DebeziumJsonDynamicSchemaFormat FORMAT = new DebeziumJsonDynamicSchemaFormat();

    private DebeziumJsonDynamicSchemaFormat() {

    }

    @SuppressWarnings("rawtypes")
    public static AbstractDynamicSchemaFormat getInstance() {
        return FORMAT;
    }

    @Override
    public JsonNode getPhysicalData(JsonNode root) {
        JsonNode physicalData = root.get(AFTER);
        if (physicalData == null) {
            physicalData = root.get(BEFORE);
        }
        return physicalData;
    }

    @Override
    public List<String> extractPrimaryKeyNames(JsonNode data) {
        List<String> pkNames = new ArrayList<>();
        JsonNode sourceNode = data.get(SOURCE);
        if (sourceNode == null) {
            return pkNames;
        }
        JsonNode pkNamesNode = sourceNode.get(PK_NAMES);
        if (pkNamesNode != null && pkNamesNode.isArray()) {
            for (int i = 0; i < pkNamesNode.size(); i++) {
                pkNames.add(pkNamesNode.get(i).asText());
            }
        }
        return pkNames;
    }

    /**
     * Get the identifier of this dynamic schema format
     *
     * @return The identifier of this dynamic schema format
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
