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

package org.apache.inlong.sort.weaviate.utils;

import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateErrorCode;
import org.apache.inlong.sort.weaviate.utils.WeaviateErrorHandler.WeaviateException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Type converter for Weaviate connector that handles data type conversions
 * between
 * Flink data types and Weaviate object properties.
 * 
 * This class provides bidirectional conversion:
 * - RowData to Weaviate object (Map&lt;String, Object&gt;) for sink operations
 * - Weaviate object to RowData for source operations
 * - Vector data conversion (String, List to float[] array)
 * - Data validation and error handling
 */
public class WeaviateTypeConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WeaviateTypeConverter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private final RowType rowType;
    private final String vectorField;
    private final Integer vectorDimension;

    /**
     * Constructor for WeaviateTypeConverter.
     *
     * @param rowType         The row type definition for the table
     * @param vectorField     The name of the vector field (optional)
     * @param vectorDimension The expected dimension of vector field (optional)
     */
    public WeaviateTypeConverter(RowType rowType, String vectorField, Integer vectorDimension) {
        this.rowType = rowType;
        this.vectorField = vectorField;
        this.vectorDimension = vectorDimension;
    }

    /**
     * Converts a RowData object to a Weaviate object with enhanced error handling.
     * This is used for sink operations when writing data to Weaviate.
     *
     * @param rowData The Flink RowData to convert
     * @return A Map representing the Weaviate object
     * @throws WeaviateException if conversion fails
     */
    public Map<String, Object> convertRowDataToWeaviateObject(RowData rowData) throws WeaviateException {
        if (rowData == null) {
            throw new WeaviateException(WeaviateErrorCode.NULL_DATA_ERROR,
                    WeaviateErrorHandler.createContext("WeaviateTypeConverter.convertRowDataToWeaviateObject",
                            "reason", "RowData is null"));
        }

        Map<String, Object> weaviateObject = new HashMap<>();
        List<RowType.RowField> fields = rowType.getFields();
        String context = WeaviateErrorHandler.createContext("convertRowDataToWeaviateObject",
                "fieldCount", fields.size());

        LOG.debug("Converting RowData to Weaviate object: {}", context);

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();

            if (rowData.isNullAt(i)) {
                // Handle null values based on configuration
                weaviateObject.put(fieldName, null);
                LOG.debug("Field '{}' is null, setting to null in Weaviate object", fieldName);
                continue;
            }

            try {
                Object value = convertFlinkValueToWeaviateValue(rowData, i, fieldType, fieldName);
                weaviateObject.put(fieldName, value);
                LOG.debug("Successfully converted field '{}' of type {} to value: {}",
                        fieldName, fieldType.getTypeRoot(), value != null ? value.getClass().getSimpleName() : "null");
            } catch (WeaviateException e) {
                // Re-throw Weaviate exceptions with additional context
                String fieldContext = WeaviateErrorHandler.createContext("field conversion",
                        "fieldName", fieldName,
                        "position", i,
                        "fieldType", fieldType.getTypeRoot());
                throw new WeaviateException(e.getErrorCode(), fieldContext + " - " + e.getContext(), e);
            } catch (Exception e) {
                String fieldContext = WeaviateErrorHandler.createContext("field conversion",
                        "fieldName", fieldName,
                        "position", i,
                        "fieldType", fieldType.getTypeRoot());
                LOG.error("Failed to convert field '{}' at position {} with type {}", fieldName, i, fieldType, e);
                throw new WeaviateException(WeaviateErrorCode.DATA_CONVERSION_FAILED, fieldContext, e);
            }
        }

        LOG.debug("Successfully converted RowData to Weaviate object with {} fields", weaviateObject.size());
        return weaviateObject;
    }

    /**
     * Converts a Weaviate object to a RowData object with enhanced error handling.
     * This is used for source operations when reading data from Weaviate.
     *
     * @param weaviateObject The Weaviate object to convert
     * @return A GenericRowData representing the converted data
     * @throws WeaviateException if conversion fails
     */
    public GenericRowData convertWeaviateObjectToRowData(Map<String, Object> weaviateObject) throws WeaviateException {
        if (weaviateObject == null) {
            throw new WeaviateException(WeaviateErrorCode.NULL_DATA_ERROR,
                    WeaviateErrorHandler.createContext("WeaviateTypeConverter.convertWeaviateObjectToRowData",
                            "reason", "Weaviate object is null"));
        }

        List<RowType.RowField> fields = rowType.getFields();
        GenericRowData rowData = new GenericRowData(fields.size());
        String context = WeaviateErrorHandler.createContext("convertWeaviateObjectToRowData",
                "fieldCount", fields.size(),
                "objectKeys", weaviateObject.keySet().size());

        LOG.debug("Converting Weaviate object to RowData: {}", context);

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();

            Object weaviateValue = weaviateObject.get(fieldName);

            try {
                Object convertedValue = convertWeaviateValueToFlinkValue(weaviateValue, fieldType, fieldName);
                rowData.setField(i, convertedValue);
                LOG.debug("Successfully converted Weaviate field '{}' of type {} from value: {} to Flink value",
                        fieldName, fieldType.getTypeRoot(),
                        weaviateValue != null ? weaviateValue.getClass().getSimpleName() : "null");
            } catch (WeaviateException e) {
                // Re-throw Weaviate exceptions with additional context
                String fieldContext = WeaviateErrorHandler.createContext("field conversion",
                        "fieldName", fieldName,
                        "position", i,
                        "fieldType", fieldType.getTypeRoot(),
                        "weaviateValueType", weaviateValue != null ? weaviateValue.getClass().getSimpleName() : "null");
                throw new WeaviateException(e.getErrorCode(), fieldContext + " - " + e.getContext(), e);
            } catch (Exception e) {
                String fieldContext = WeaviateErrorHandler.createContext("field conversion",
                        "fieldName", fieldName,
                        "position", i,
                        "fieldType", fieldType.getTypeRoot(),
                        "weaviateValueType", weaviateValue != null ? weaviateValue.getClass().getSimpleName() : "null");
                LOG.error("Failed to convert Weaviate field '{}' with type {} and value {}", fieldName, fieldType,
                        weaviateValue, e);
                throw new WeaviateException(WeaviateErrorCode.DATA_CONVERSION_FAILED, fieldContext, e);
            }
        }

        LOG.debug("Successfully converted Weaviate object to RowData with {} fields", fields.size());
        return rowData;
    }

    /**
     * Converts vector data from various formats to float array with enhanced error
     * handling.
     * Supports String (JSON array), List&lt;Number&gt;, and float[] formats.
     *
     * @param vectorData The vector data to convert
     * @return A float array representing the vector
     * @throws WeaviateException if conversion fails or dimension mismatch
     */
    public float[] convertToVector(Object vectorData) throws WeaviateException {
        if (vectorData == null) {
            return null;
        }

        String context = WeaviateErrorHandler.createContext("convertToVector",
                "vectorDataType", vectorData.getClass().getSimpleName(),
                "expectedDimension", vectorDimension);

        LOG.debug("Converting vector data: {}", context);

        float[] result;

        try {
            if (vectorData instanceof String) {
                result = parseVectorFromString((String) vectorData);
            } else if (vectorData instanceof List) {
                result = convertListToFloatArray((List<?>) vectorData);
            } else if (vectorData instanceof float[]) {
                result = (float[]) vectorData;
            } else if (vectorData instanceof double[]) {
                double[] doubleArray = (double[]) vectorData;
                result = new float[doubleArray.length];
                for (int i = 0; i < doubleArray.length; i++) {
                    result[i] = (float) doubleArray[i];
                }
            } else if (vectorData instanceof ArrayData) {
                result = convertArrayDataToFloatArray((ArrayData) vectorData);
            } else {
                throw new WeaviateException(WeaviateErrorCode.INVALID_VECTOR_FORMAT,
                        context + " - Unsupported vector data type: " + vectorData.getClass());
            }

            // Validate vector dimension if specified
            if (vectorDimension != null && result.length != vectorDimension) {
                throw new WeaviateException(WeaviateErrorCode.INVALID_VECTOR_DIMENSION,
                        WeaviateErrorHandler.createContext("vector dimension validation",
                                "expected", vectorDimension, "actual", result.length));
            }

            LOG.debug("Successfully converted vector data to float array with {} dimensions", result.length);
            return result;

        } catch (WeaviateException e) {
            // Re-throw Weaviate exceptions as-is
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to convert vector data: {}", context, e);
            throw new WeaviateException(WeaviateErrorCode.VECTOR_CONVERSION_FAILED, context, e);
        }
    }

    /**
     * Converts a float array to vector data in the specified format.
     * 
     * @param vector       The float array to convert
     * @param targetFormat The target format ("string", "list", or "array")
     * @return The converted vector data
     */
    public Object convertFromVector(float[] vector, String targetFormat) {
        if (vector == null) {
            return null;
        }

        switch (targetFormat.toLowerCase()) {
            case "string":
                return vectorToString(vector);
            case "list":
                List<Float> list = new ArrayList<>();
                for (float f : vector) {
                    list.add(f);
                }
                return list;
            case "array":
            default:
                return vector;
        }
    }

    /**
     * Converts a Flink value to a Weaviate value.
     */
    private Object convertFlinkValueToWeaviateValue(RowData rowData, int pos, LogicalType logicalType,
            String fieldName) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();

        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                StringData stringData = rowData.getString(pos);
                String stringValue = stringData.toString();

                // Handle vector field conversion
                if (fieldName.equals(vectorField)) {
                    return convertToVector(stringValue);
                }
                return stringValue;

            case BOOLEAN:
                return rowData.getBoolean(pos);

            case DECIMAL:
                DecimalData decimalData = rowData.getDecimal(pos, ((DecimalType) logicalType).getPrecision(),
                        ((DecimalType) logicalType).getScale());
                return decimalData.toBigDecimal();

            case TINYINT:
                return rowData.getByte(pos);

            case SMALLINT:
                return rowData.getShort(pos);

            case INTEGER:
                return rowData.getInt(pos);

            case BIGINT:
                return rowData.getLong(pos);

            case FLOAT:
                return rowData.getFloat(pos);

            case DOUBLE:
                return rowData.getDouble(pos);

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampData timestampData = rowData.getTimestamp(pos,
                        ((org.apache.flink.table.types.logical.TimestampType) logicalType).getPrecision());
                return timestampData.toLocalDateTime().format(TIMESTAMP_FORMATTER);

            case ARRAY:
                ArrayData arrayData = rowData.getArray(pos);
                ArrayType arrayType = (ArrayType) logicalType;
                LogicalType elementType = arrayType.getElementType();

                // Handle vector array conversion
                if (fieldName.equals(vectorField) && elementType.getTypeRoot() == LogicalTypeRoot.FLOAT) {
                    return convertArrayDataToFloatArray(arrayData);
                }

                return convertArrayDataToList(arrayData, elementType);

            case ROW:
                RowData nestedRowData = rowData.getRow(pos, ((RowType) logicalType).getFieldCount());
                return convertNestedRowDataToMap(nestedRowData, (RowType) logicalType);

            default:
                throw new IllegalArgumentException("Unsupported Flink type: " + typeRoot);
        }
    }

    /**
     * Converts a Weaviate value to a Flink value.
     */
    private Object convertWeaviateValueToFlinkValue(Object weaviateValue, LogicalType logicalType, String fieldName) {
        if (weaviateValue == null) {
            return null;
        }

        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();

        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                if (fieldName.equals(vectorField) && weaviateValue instanceof List) {
                    // Convert vector list back to string representation
                    return StringData.fromString(vectorToString(convertListToFloatArray((List<?>) weaviateValue)));
                }
                return StringData.fromString(weaviateValue.toString());

            case BOOLEAN:
                if (weaviateValue instanceof Boolean) {
                    return (Boolean) weaviateValue;
                }
                return Boolean.parseBoolean(weaviateValue.toString());

            case DECIMAL:
                if (weaviateValue instanceof BigDecimal) {
                    return DecimalData.fromBigDecimal((BigDecimal) weaviateValue,
                            ((DecimalType) logicalType).getPrecision(), ((DecimalType) logicalType).getScale());
                }
                return DecimalData.fromBigDecimal(new BigDecimal(weaviateValue.toString()),
                        ((DecimalType) logicalType).getPrecision(), ((DecimalType) logicalType).getScale());

            case TINYINT:
                return ((Number) weaviateValue).byteValue();

            case SMALLINT:
                return ((Number) weaviateValue).shortValue();

            case INTEGER:
                return ((Number) weaviateValue).intValue();

            case BIGINT:
                return ((Number) weaviateValue).longValue();

            case FLOAT:
                return ((Number) weaviateValue).floatValue();

            case DOUBLE:
                return ((Number) weaviateValue).doubleValue();

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalDateTime dateTime;
                if (weaviateValue instanceof String) {
                    try {
                        dateTime = LocalDateTime.parse((String) weaviateValue, TIMESTAMP_FORMATTER);
                    } catch (DateTimeParseException e) {
                        // Try alternative formats
                        dateTime = LocalDateTime.parse((String) weaviateValue);
                    }
                } else {
                    throw new IllegalArgumentException("Cannot convert " + weaviateValue.getClass() + " to timestamp");
                }
                return TimestampData.fromLocalDateTime(dateTime);

            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                LogicalType elementType = arrayType.getElementType();

                if (fieldName.equals(vectorField) && weaviateValue instanceof List) {
                    // Convert vector list to ArrayData
                    float[] vectorArray = convertListToFloatArray((List<?>) weaviateValue);
                    Object[] objectArray = new Object[vectorArray.length];
                    for (int i = 0; i < vectorArray.length; i++) {
                        objectArray[i] = vectorArray[i];
                    }
                    return new GenericArrayData(objectArray);
                }

                if (weaviateValue instanceof List) {
                    return convertListToArrayData((List<?>) weaviateValue, elementType);
                }
                throw new IllegalArgumentException("Cannot convert " + weaviateValue.getClass() + " to array");

            case ROW:
                if (weaviateValue instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mapValue = (Map<String, Object>) weaviateValue;
                    return convertMapToRowData(mapValue, (RowType) logicalType);
                }
                throw new IllegalArgumentException("Cannot convert " + weaviateValue.getClass() + " to row");

            default:
                throw new IllegalArgumentException("Unsupported Flink type: " + typeRoot);
        }
    }

    /**
     * Parses vector from string representation (JSON array format).
     */
    private float[] parseVectorFromString(String vectorString) throws JsonProcessingException {
        if (vectorString.trim().startsWith("[")) {
            // JSON array format
            JsonNode jsonNode = OBJECT_MAPPER.readTree(vectorString);
            if (!jsonNode.isArray()) {
                throw new IllegalArgumentException("Vector string must be a JSON array");
            }

            float[] result = new float[jsonNode.size()];
            for (int i = 0; i < jsonNode.size(); i++) {
                result[i] = (float) jsonNode.get(i).asDouble();
            }
            return result;
        } else {
            // Comma-separated values
            String[] parts = vectorString.split(",");
            float[] result = new float[parts.length];
            for (int i = 0; i < parts.length; i++) {
                result[i] = Float.parseFloat(parts[i].trim());
            }
            return result;
        }
    }

    /**
     * Converts a List to float array.
     */
    private float[] convertListToFloatArray(List<?> list) {
        float[] result = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (item instanceof Number) {
                result[i] = ((Number) item).floatValue();
            } else {
                result[i] = Float.parseFloat(item.toString());
            }
        }
        return result;
    }

    /**
     * Converts ArrayData to float array.
     */
    private float[] convertArrayDataToFloatArray(ArrayData arrayData) {
        float[] result = new float[arrayData.size()];
        for (int i = 0; i < arrayData.size(); i++) {
            result[i] = arrayData.getFloat(i);
        }
        return result;
    }

    /**
     * Converts float array to string representation.
     */
    private String vectorToString(float[] vector) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(vector[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Converts ArrayData to List.
     */
    private List<Object> convertArrayDataToList(ArrayData arrayData, LogicalType elementType) {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < arrayData.size(); i++) {
            Object element = convertArrayElementToObject(arrayData, i, elementType);
            result.add(element);
        }
        return result;
    }

    /**
     * Converts array element to object based on type.
     */
    private Object convertArrayElementToObject(ArrayData arrayData, int index, LogicalType elementType) {
        if (arrayData.isNullAt(index)) {
            return null;
        }

        LogicalTypeRoot typeRoot = elementType.getTypeRoot();
        switch (typeRoot) {
            case VARCHAR:
            case CHAR:
                return arrayData.getString(index).toString();
            case BOOLEAN:
                return arrayData.getBoolean(index);
            case TINYINT:
                return arrayData.getByte(index);
            case SMALLINT:
                return arrayData.getShort(index);
            case INTEGER:
                return arrayData.getInt(index);
            case BIGINT:
                return arrayData.getLong(index);
            case FLOAT:
                return arrayData.getFloat(index);
            case DOUBLE:
                return arrayData.getDouble(index);
            default:
                throw new IllegalArgumentException("Unsupported array element type: " + typeRoot);
        }
    }

    /**
     * Converts List to ArrayData.
     */
    private ArrayData convertListToArrayData(List<?> list, LogicalType elementType) {
        Object[] array = new Object[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = convertObjectToArrayElement(list.get(i), elementType);
        }
        return new GenericArrayData(array);
    }

    /**
     * Converts object to array element based on type.
     */
    private Object convertObjectToArrayElement(Object obj, LogicalType elementType) {
        if (obj == null) {
            return null;
        }

        LogicalTypeRoot typeRoot = elementType.getTypeRoot();
        switch (typeRoot) {
            case VARCHAR:
            case CHAR:
                return StringData.fromString(obj.toString());
            case BOOLEAN:
                return obj instanceof Boolean ? (Boolean) obj : Boolean.parseBoolean(obj.toString());
            case TINYINT:
                return ((Number) obj).byteValue();
            case SMALLINT:
                return ((Number) obj).shortValue();
            case INTEGER:
                return ((Number) obj).intValue();
            case BIGINT:
                return ((Number) obj).longValue();
            case FLOAT:
                return ((Number) obj).floatValue();
            case DOUBLE:
                return ((Number) obj).doubleValue();
            default:
                throw new IllegalArgumentException("Unsupported array element type: " + typeRoot);
        }
    }

    /**
     * Converts nested RowData to Map.
     */
    private Map<String, Object> convertNestedRowDataToMap(RowData rowData, RowType rowType) {
        Map<String, Object> result = new HashMap<>();
        List<RowType.RowField> fields = rowType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();

            if (!rowData.isNullAt(i)) {
                Object value = convertFlinkValueToWeaviateValue(rowData, i, fieldType, fieldName);
                result.put(fieldName, value);
            }
        }

        return result;
    }

    /**
     * Converts Map to RowData.
     */
    private RowData convertMapToRowData(Map<String, Object> map, RowType rowType) {
        List<RowType.RowField> fields = rowType.getFields();
        GenericRowData rowData = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            String fieldName = field.getName();
            LogicalType fieldType = field.getType();

            Object mapValue = map.get(fieldName);
            Object convertedValue = convertWeaviateValueToFlinkValue(mapValue, fieldType, fieldName);
            rowData.setField(i, convertedValue);
        }

        return rowData;
    }

    /**
     * Validates the converter configuration with enhanced error handling.
     */
    public void validate() throws WeaviateException {
        String context = WeaviateErrorHandler.createContext("WeaviateTypeConverter.validate",
                "vectorField", vectorField,
                "vectorDimension", vectorDimension);

        LOG.debug("Validating type converter configuration: {}", context);

        if (rowType == null) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_CONFIG,
                    context + " - RowType cannot be null");
        }

        if (vectorField != null) {
            boolean vectorFieldExists = rowType.getFields().stream()
                    .anyMatch(field -> field.getName().equals(vectorField));
            if (!vectorFieldExists) {
                String availableFields = rowType.getFields().stream()
                        .map(RowType.RowField::getName)
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("none");
                throw new WeaviateException(WeaviateErrorCode.SCHEMA_MISMATCH,
                        WeaviateErrorHandler.createContext("vector field validation",
                                "vectorField", vectorField,
                                "availableFields", availableFields));
            }
        }

        if (vectorDimension != null && vectorDimension <= 0) {
            throw new WeaviateException(WeaviateErrorCode.INVALID_VECTOR_DIMENSION,
                    context + " - Vector dimension must be positive, got: " + vectorDimension);
        }

        LOG.info("Type converter configuration validated successfully: {}", context);
    }

    // Getters for configuration
    public RowType getRowType() {
        return rowType;
    }

    public String getVectorField() {
        return vectorField;
    }

    public Integer getVectorDimension() {
        return vectorDimension;
    }
}