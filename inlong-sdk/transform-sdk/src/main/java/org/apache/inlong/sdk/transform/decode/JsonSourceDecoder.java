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

package org.apache.inlong.sdk.transform.decode;

import org.apache.inlong.sdk.transform.pojo.JsonSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * JsonSourceDecoder
 * 
 */
public class JsonSourceDecoder extends SourceDecoder<String> {

    protected JsonSourceInfo sourceInfo;
    private Charset srcCharset = Charset.defaultCharset();
    private String rowsNodePath;
    private List<JsonNode> childNodes;

    private Gson gson = new Gson();

    /**
     * Constructor
     * @param sourceInfo
     */
    public JsonSourceDecoder(JsonSourceInfo sourceInfo) {
        this.sourceInfo = sourceInfo;
        if (!StringUtils.isBlank(sourceInfo.getCharset())) {
            this.srcCharset = Charset.forName(sourceInfo.getCharset());
        }
        this.rowsNodePath = sourceInfo.getRowsNodePath();
        if (!StringUtils.isBlank(rowsNodePath)) {
            this.childNodes = new ArrayList<>();
            String[] nodeStrings = this.rowsNodePath.split("\\.");
            for (String nodeString : nodeStrings) {
                this.childNodes.add(new JsonNode(nodeString));
            }
        }
    }

    /**
     * decode
     * @param srcBytes
     * @param context
     * @return
     */
    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        String srcString = new String(srcBytes, srcCharset);
        return this.decode(srcString, context);
    }

    /**
     * decode
     * @param srcString
     * @param context
     * @return
     */
    @Override
    public SourceData decode(String srcString, Context context) {
        JsonObject root = gson.fromJson(srcString, JsonObject.class);
        JsonArray childRoot = null;
        if (CollectionUtils.isEmpty(childNodes)) {
            return new JsonSourceData(root, null, context);
        }
        JsonElement current = root;
        for (JsonNode node : childNodes) {
            if (!current.isJsonObject()) {
                // error data
                return new JsonSourceData(root, null, context);
            }
            JsonElement newElement = current.getAsJsonObject().get(node.getName());
            if (newElement == null) {
                // error data
                return new JsonSourceData(root, null, context);
            }
            // node is not array
            if (!node.isArray()) {
                current = newElement;
                continue;
            }
            // node is an array
            current = getElementFromArray(node, newElement);
            if (current == null) {
                // error data
                return new JsonSourceData(root, null, context);
            }
        }
        if (!current.isJsonArray()) {
            // error data
            return new JsonSourceData(root, null, context);
        }
        childRoot = current.getAsJsonArray();
        return new JsonSourceData(root, childRoot, context);
    }

    private JsonElement getElementFromArray(JsonNode node, JsonElement curElement) {
        if (node.getArrayIndices().isEmpty()) {
            // error data
            return null;
        }
        for (int index : node.getArrayIndices()) {
            if (!curElement.isJsonArray()) {
                // error data
                return null;
            }
            JsonArray newArray = curElement.getAsJsonArray();
            if (index >= newArray.size()) {
                // error data
                return null;
            }
            curElement = newArray.get(index);
        }
        return curElement;
    }
}
