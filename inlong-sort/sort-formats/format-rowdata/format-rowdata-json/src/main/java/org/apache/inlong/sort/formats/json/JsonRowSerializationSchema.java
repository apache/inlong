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
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BooleanFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ByteFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DecimalFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DoubleFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FloatFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.MapFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ShortFormatInfo;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.inlong.sort.formats.base.TextFormatOptions.CHARSET;

/**
 * The serializer for the records in json format.
 */
public class JsonRowSerializationSchema implements SerializationSchema<Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonRowSerializationSchema.class);

    private static final long serialVersionUID = -6616673074377303063L;

    /**
     * Format information describing the result type.
     */
    @Nonnull
    private final RowFormatInfo rowFormatInfo;

    /**
     * The charset of the text.
     */
    @Nonnull
    private final String charset;

    /** Object mapper that is used to create output JSON objects. */
    @Nonnull
    private final ObjectMapper objectMapper;

    public JsonRowSerializationSchema(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String charset,
            @Nonnull Configuration configuration) {
        this.rowFormatInfo = rowFormatInfo;
        this.charset = charset;
        this.objectMapper = JsonFormatUtils.createObjectMapper(configuration);
    }

    @Override
    public byte[] serialize(Row input) {
        try {
            JsonNode root = convert(rowFormatInfo, "", input);
            String text = objectMapper.writeValueAsString(root);

            return text.getBytes(Charset.forName(charset));
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly serialize the input to json. Input=[%s].", input), t);
        }
    }

    private JsonNode convert(FormatInfo formatInfo, String path, Object object) {
        if (formatInfo instanceof BasicFormatInfo) {
            return convertBasic((BasicFormatInfo<?>) formatInfo, path, object);
        } else if (formatInfo instanceof ArrayFormatInfo) {
            return convertArray((ArrayFormatInfo) formatInfo, path, object);
        } else if (formatInfo instanceof MapFormatInfo) {
            return convertMap((MapFormatInfo) formatInfo, path, object);
        } else if (formatInfo instanceof RowFormatInfo) {
            return convert((RowFormatInfo) formatInfo, path, object);
        } else {
            throw new IllegalStateException(
                    String.format("unsupported format info %s (path=[%s], data=[%s]).",
                            formatInfo.getClass(), path, object));
        }
    }

    @SuppressWarnings("unchecked")
    private JsonNode convertBasic(BasicFormatInfo<?> basicFormatInfo, String path, Object object) {
        if (object == null) {
            return objectMapper.getNodeFactory().nullNode();
        }

        try {
            if (basicFormatInfo instanceof BooleanFormatInfo) {
                return objectMapper.getNodeFactory().booleanNode((Boolean) object);
            } else if (basicFormatInfo instanceof ByteFormatInfo ||
                    basicFormatInfo instanceof ShortFormatInfo ||
                    basicFormatInfo instanceof IntFormatInfo ||
                    basicFormatInfo instanceof LongFormatInfo) {
                return objectMapper.getNodeFactory().numberNode(((Number) object).longValue());
            } else if (basicFormatInfo instanceof FloatFormatInfo) {
                return objectMapper.getNodeFactory().numberNode((Float) object);
            } else if (basicFormatInfo instanceof DoubleFormatInfo) {
                return objectMapper.getNodeFactory().numberNode((Double) object);
            } else if (basicFormatInfo instanceof DecimalFormatInfo) {
                return objectMapper.getNodeFactory().numberNode((BigDecimal) object);
            } else {
                String text = ((BasicFormatInfo<Object>) basicFormatInfo).serialize(object);
                return objectMapper.getNodeFactory().textNode(text);
            }
        } catch (Throwable t) {
            throw new RuntimeException(
                    String.format("Could not properly serialize the value (path=[%s], data=[%s]).",
                            path, object),
                    t);
        }
    }

    private JsonNode convertArray(ArrayFormatInfo arrayFormatInfo, String path, Object object) {
        checkState(object.getClass().isArray(),
                String.format(
                        "The object is not typed array, actual type is [%s] (path=[%s], data=[%s]).",
                        object.getClass(), path, object));

        Object[] array = (Object[]) object;

        ArrayNode arrayNode = objectMapper.createArrayNode();
        for (int i = 0; i < array.length; ++i) {
            String childPath = path + "[" + i + "]";
            Object childObject = array[i];
            arrayNode.add(convert(arrayFormatInfo.getElementFormatInfo(), childPath, childObject));
        }

        return arrayNode;
    }

    @SuppressWarnings("unchecked")
    private JsonNode convertMap(MapFormatInfo mapFormatInfo, String path, Object object) {
        checkState(mapFormatInfo.getKeyFormatInfo() instanceof BasicFormatInfo,
                String.format("The key field format of map must be instanceof BasicFormatInfo, " +
                        "actual is [%s] (path=[%s], data=[%s]).",
                        mapFormatInfo.getKeyFormatInfo().getClass(), path, object));

        BasicFormatInfo<Object> keyFormatInfo =
                (BasicFormatInfo<Object>) mapFormatInfo.getKeyFormatInfo();
        FormatInfo valueFormatInfo = mapFormatInfo.getValueFormatInfo();

        checkState(object instanceof Map,
                String.format("The object must be not typed map, actual type is [%s] (path=[%s], " +
                        "data=[%s]).", object.getClass(), path, object));
        Map<Object, Object> map = (Map<Object, Object>) object;

        ObjectNode node = objectMapper.createObjectNode();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            Object key = entry.getKey();
            String keyText;
            try {
                keyText = keyFormatInfo.serialize(entry.getKey());
            } catch (Throwable t) {
                throw new RuntimeException(
                        String.format("Could not properly deserialize the key (path=[%s], data=[%s]).",
                                path, key),
                        t);
            }

            String childPath = path + "<" + keyText + ">";
            Object childObject = entry.getValue();
            JsonNode value = convert(valueFormatInfo, childPath, childObject);

            node.set(keyText, value);
        }

        return node;
    }

    private JsonNode convert(RowFormatInfo rowFormatInfo, String path, Object object) {
        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        checkState(object instanceof Row,
                String.format("The object is not typed row, actual type is [%s] (path=[%s], " +
                        "data=[%s]).", object.getClass(), path, object));

        Row row = (Row) object;

        if (row.getArity() != fieldFormatInfos.length) {
            LOGGER.warn("The number of fields mismatches: expected=[{}], actual=[{}] (path=[{}], " +
                    "data=[{}].", fieldFormatInfos.length, row.getArity(), path, object);
        }

        ObjectNode node = objectMapper.createObjectNode();

        int numActualFields = Math.min(row.getArity(), fieldFormatInfos.length);
        for (int i = 0; i < numActualFields; i++) {
            String fieldName = fieldNames[i];

            String childPath = path + "." + fieldName;
            Object childObject = row.getField(i);

            if (childObject == null) {
                continue;
            }

            node.set(fieldName, convert(fieldFormatInfos[i], childPath, childObject));
        }

        return node;
    }

    /**
     * Builder for {@link JsonRowSerializationSchema}.
     */
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

        public JsonRowSerializationSchema build() {
            return new JsonRowSerializationSchema(rowFormatInfo, charset, configuration);
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

        JsonRowSerializationSchema that = (JsonRowSerializationSchema) object;
        return rowFormatInfo.equals(that.rowFormatInfo) && charset.equals(that.charset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, charset);
    }
}
