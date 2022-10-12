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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Json dynamic format class
 * This class main handle:
 * 1. deserialize data from byte array
 * 2. parse pattern and get the real value from the raw data(contains meta data and physical data)
 * Such as:
 * 1). give a pattern "${a}{b}{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be '123'
 * 2). give a pattern "${a}_{b}_{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be '1_2_3'
 * 3). give a pattern "prefix_${a}_{b}_{c}_suffix" and the root Node contains the keys(a: '1', b: '2', c: '3')
 * the result of pared will be 'prefix_1_2_3_suffix'
 */
public abstract class JsonDynamicSchemaFormat extends AbstractDynamicSchemaFormat<JsonNode> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Extract value by key from the raw data
     *
     * @param message The byte array of raw data
     * @param keys The key list that will be used to extract
     * @return The value list maps the keys
     * @throws IOException The exceptions may throws when extract
     */
    @Override
    public List<String> extract(byte[] message, String... keys) throws IOException {
        if (keys == null || keys.length == 0) {
            return new ArrayList<>();
        }
        final JsonNode root = deserialize(message);
        JsonNode physicalNode = getPhysicalData(root);
        List<String> values = new ArrayList<>(keys.length);
        if (physicalNode == null) {
            for (String key : keys) {
                values.add(extract(root, key));
            }
            return values;
        }
        for (String key : keys) {
            String value = extract(physicalNode, key);
            if (value == null) {
                value = extract(root, key);
            }
            values.add(value);
        }
        return values;
    }

    /**
     * Extract value by key from ${@link JsonNode}
     *
     * @param jsonNode The json node
     * @param key The key that will be used to extract
     * @return The value maps the key in the json node
     */
    @Override
    public String extract(JsonNode jsonNode, String key) {
        if (jsonNode == null || key == null) {
            return null;
        }
        JsonNode value = jsonNode.get(key);
        if (value != null) {
            return value.asText();
        }
        int index = key.indexOf(".");
        if (index > 0 && index + 1 < key.length()) {
            return extract(jsonNode.get(key.substring(0, index)), key.substring(index + 1));
        }
        return null;
    }

    /**
     * Deserialize from byte array and return a ${@link JsonNode}
     *
     * @param message The byte array of raw data
     * @return The JsonNode
     * @throws IOException The exceptions may throws when deserialize
     */
    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    /**
     * Parse msg and replace the value by key from meta data and physical.
     * See details {@link JsonDynamicSchemaFormat#parse(JsonNode, String)}
     *
     * @param message The source of data rows format by bytes
     * @param pattern The pattern value
     * @return The result of parsed
     * @throws IOException The exception will throws
     */
    @Override
    public String parse(byte[] message, String pattern) throws IOException {
        return parse(deserialize(message), pattern);
    }

    /**
     * Parse msg and replace the value by key from meta data and physical.
     * Such as:
     * 1. give a pattern "${a}{b}{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be '123'
     * 2. give a pattern "${a}_{b}_{c}" and the root Node contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be '1_2_3'
     * 3. give a pattern "prefix_${a}_{b}_{c}_suffix" and the root Node contains the keys(a: '1', b: '2', c: '3')
     * the result of pared will be 'prefix_1_2_3_suffix'
     *
     * @param rootNode The root node of json
     * @param pattern The pattern value
     * @return The result of parsed
     * @throws IOException The exception will throws
     */
    @Override
    public String parse(JsonNode rootNode, String pattern) throws IOException {
        Matcher matcher = PATTERN.matcher(pattern);
        StringBuffer sb = new StringBuffer();
        JsonNode physicalNode = getPhysicalData(rootNode);
        while (matcher.find()) {
            String keyText = matcher.group(1);
            String replacement = extract(physicalNode, keyText);
            if (replacement == null) {
                replacement = extract(rootNode, keyText);
            }
            if (replacement == null) {
                replacement = "";
            }
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Get physical data from the root json node
     *
     * @param root The json root node
     * @return The physical data node
     */
    protected abstract JsonNode getPhysicalData(JsonNode root);
}
