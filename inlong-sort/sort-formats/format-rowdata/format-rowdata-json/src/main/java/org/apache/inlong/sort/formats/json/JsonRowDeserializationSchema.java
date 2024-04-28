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

package org.apache.inlong.sort.formats.json;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.ArrayFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BasicFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.MapFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.sort.formats.base.TableFormatUtils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.CHARSET;
import static org.apache.inlong.sort.formats.json.JsonFormatUtils.createObjectMapper;

/**
 * The deserializer for the records in json format.
 */
public class JsonRowDeserializationSchema implements DeserializationSchema<Row> {

    private static final long serialVersionUID = -3876113576750234901L;

    /**
     * Format information describing the result type.
     */
    @Nonnull
    protected final RowFormatInfo rowFormatInfo;

    /**
     * The charset of the text.
     */
    @Nonnull
    protected final String charset;

    /**
     * Object mapper for parsing the JSON.
     */
    @Nonnull
    protected final ObjectMapper objectMapper;

    public JsonRowDeserializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Configuration configuration) {
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.objectMapper = createObjectMapper(configuration);
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        // noinspection unchecked
        return (TypeInformation<Row>) TableFormatUtils.getType(rowFormatInfo.getTypeInfo());
    }

    @Override
    public boolean isEndOfStream(Row row) {
        return false;
    }

    @Override
    public Row deserialize(byte[] bytes) {
        String text = new String(bytes, Charset.forName(charset));

        try {
            final JsonNode root = objectMapper.readTree(text);
            return convert(rowFormatInfo, "", root);
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly deserialize json. Text=[%s].", text), t);
        }
    }

    protected Object convert(FormatInfo formatInfo, String path, JsonNode node) throws IOException {
        if (formatInfo instanceof BasicFormatInfo) {
            return convertBasic((BasicFormatInfo<?>) formatInfo, path, node);
        } else if (formatInfo instanceof ArrayFormatInfo) {
            FormatInfo elemFormatInfo = ((ArrayFormatInfo) formatInfo).getElementFormatInfo();
            // As flink treat StringArrayTypeInfo and ObjectArrayTypeInfo separately, so
            // we should follow this constraints in flink formats
            if (elemFormatInfo instanceof StringFormatInfo) {
                // string array
                return convertStringArray((ArrayFormatInfo) formatInfo, path, node);
            } else {
                // object array
                return convertArray((ArrayFormatInfo) formatInfo, path, node);
            }
        } else if (formatInfo instanceof MapFormatInfo) {
            return convertMap((MapFormatInfo) formatInfo, path, node);
        } else if (formatInfo instanceof RowFormatInfo) {
            return convert((RowFormatInfo) formatInfo, path, node);
        } else {
            throw new IllegalStateException(
                    String.format("unsupported format info %s (path=[%s], text=[%s]).",
                            formatInfo.getClass(), path, node));
        }
    }

    private Object convertBasic(BasicFormatInfo<?> basicFormatInfo, String path, JsonNode node) throws IOException {

        String text = node.asText();

        try {
            return basicFormatInfo.deserialize(text);
        } catch (Throwable t) {
            throw new IOException(
                    String.format("Could not properly deserialize the value (path=[%s], text=[%s]).",
                            path, text),
                    t);
        }
    }

    private Object[] convertArray(ArrayFormatInfo arrayFormatInfo, String path, JsonNode node) throws IOException {
        final FormatInfo elementFormatInfo = arrayFormatInfo.getElementFormatInfo();

        checkState(node.isArray(),
                String.format("The json node is not array, actual type is [%s] (path=[%s], text=[%s]).",
                        node.getNodeType(), path, node));

        final ArrayNode arrayNode = (ArrayNode) node;

        final int nodeSize = arrayNode.size();
        final Object[] array = new Object[nodeSize];

        for (int i = 0; i < nodeSize; i++) {
            String childPath = path + "[" + i + "]";
            JsonNode childNode = arrayNode.get(i);
            array[i] = convert(elementFormatInfo, childPath, childNode);
        }

        return array;
    }

    private String[] convertStringArray(ArrayFormatInfo arrayFormatInfo, String path, JsonNode node)
            throws IOException {
        final FormatInfo elementFormatInfo = arrayFormatInfo.getElementFormatInfo();

        checkState(node.isArray(),
                String.format("The json node is not array, actual type is [%s] (path=[%s], text=[%s]).",
                        node.getNodeType(), path, node));

        final ArrayNode arrayNode = (ArrayNode) node;

        final int nodeSize = arrayNode.size();
        final String[] array = new String[nodeSize];

        for (int i = 0; i < nodeSize; i++) {
            String childPath = path + "[" + i + "]";
            JsonNode childNode = arrayNode.get(i);
            Object childValue = convert(elementFormatInfo, childPath, childNode);
            checkState(childValue instanceof String,
                    String.format("The child json node is not string, actual type is [%s] (path=[%s]," +
                            " text=[%s]).", childNode.getNodeType(), childPath, childNode));
            array[i] = (String) childValue;
        }
        return array;
    }

    private Map<Object, Object> convertMap(MapFormatInfo mapFormatInfo, String path, JsonNode node) throws IOException {
        checkState(mapFormatInfo.getKeyFormatInfo() instanceof BasicFormatInfo,
                String.format("The key field format of map must be instanceof BasicFormatInfo, " +
                        "actual type is [%s] (path=[%s], text=[%s]).",
                        mapFormatInfo.getKeyFormatInfo().getClass(), path, node));

        final BasicFormatInfo<?> keyFormatInfo =
                (BasicFormatInfo<?>) mapFormatInfo.getKeyFormatInfo();
        final FormatInfo valueFormatInfo = mapFormatInfo.getValueFormatInfo();

        checkState(node.isObject(),
                String.format("The json node is not object, actual type is [%s] (path=[%s], " +
                        "text=[%s]).", node.getNodeType(), path, node));

        final int nodeSize = node.size();
        final Map<Object, Object> map = new HashMap<>(nodeSize);

        Iterator<String> childFieldNames = node.fieldNames();
        while (childFieldNames.hasNext()) {
            String childFieldName = childFieldNames.next();

            Object key;
            try {
                key = keyFormatInfo.deserialize(childFieldName);
            } catch (Throwable t) {
                throw new IOException(
                        String.format("Could not properly deserialize the key (path=[%s], text=[%s]).",
                                path, childFieldName),
                        t);
            }

            String childPath = path + "<" + childFieldName + ">";
            JsonNode childNode = node.get(childFieldName);
            Object value = convert(valueFormatInfo, childPath, childNode);

            map.put(key, value);
        }

        return map;
    }

    protected Row convert(
            RowFormatInfo rowFormatInfo,
            String path,
            JsonNode node) throws IOException {
        checkState(node.isObject(),
                String.format("The json node is not object, actual type is [%s] (path=[%s], text=[%s]).",
                        node.getNodeType(), path, node));

        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        final int arity = fieldNames.length;
        Row row = new Row(arity);
        for (int i = 0; i < arity; i++) {
            String fieldName = fieldNames[i];
            String childPath = path + "." + fieldName;
            JsonNode childNode = node.get(fieldName);

            if (childNode == null) {
                row.setField(i, null);
            } else {
                row.setField(i, convert(fieldFormatInfos[i], childPath, childNode));
            }
        }

        return row;
    }

    /** Builder for {@link JsonRowDeserializationSchema}. */
    public static class Builder {

        private final RowFormatInfo rowFormatInfo;

        private final Configuration configuration = new Configuration();

        private String charset = CHARSET.defaultValue();

        public Builder(RowFormatInfo rowFormatInfo) {
            checkNotNull(rowFormatInfo);
            this.rowFormatInfo = rowFormatInfo;
        }

        public Builder setCharset(String charset) {
            this.charset = charset;
            return this;
        }

        @SuppressWarnings("UnusedReturnValue")
        public Builder setConfiguration(Configuration configuration) {
            this.configuration.addAll(configuration);
            return this;
        }

        public JsonRowDeserializationSchema build() {
            return new JsonRowDeserializationSchema(rowFormatInfo, charset, configuration);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        JsonRowDeserializationSchema that = (JsonRowDeserializationSchema) object;
        return rowFormatInfo.equals(that.rowFormatInfo) &&
                charset.equals(that.charset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, charset);
    }
}
