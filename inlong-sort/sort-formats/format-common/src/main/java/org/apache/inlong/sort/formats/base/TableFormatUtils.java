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

package org.apache.inlong.sort.formats.base;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.ArrayFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ArrayTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BasicFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BinaryFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BinaryTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BooleanFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BooleanTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ByteFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ByteTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DateTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DecimalFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DecimalTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DoubleFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.DoubleTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FloatFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FloatTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatUtils;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.IntTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LocalZonedTimestampFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LocalZonedTimestampTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.LongTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.MapFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.MapTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.NullFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ShortFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.ShortTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.StringTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimeFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimeTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimestampFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TimestampTypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.TypeInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.VarBinaryFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.VarCharFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DERIVE_SCHEMA;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_PROPERTY_VERSION;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_SCHEMA;

/**
 * A utility class for table formats.
 */
public class TableFormatUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableFormatUtils.class);
    private static final String SCHEMA = "schema";
    private static final String SCHEMA_PROCTIME = "proctime";
    private static final String SCHEMA_FROM = "from";
    private static final String ROWTIME_TIMESTAMPS_TYPE = "rowtime.timestamps.type";
    private static final String ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD = "from-field";
    private static final String ROWTIME_TIMESTAMPS_FROM = "rowtime.timestamps.from";

    /**
     * Derive the format information for the given type.
     *
     * @param logicalType The type whose format is derived.
     * @return The format information for the given type.
     */
    public static FormatInfo deriveFormatInfo(LogicalType logicalType) {
        if (logicalType instanceof VarCharType) {
            return StringFormatInfo.INSTANCE;
        } else if (logicalType instanceof BooleanType) {
            return BooleanFormatInfo.INSTANCE;
        } else if (logicalType instanceof TinyIntType) {
            return ByteFormatInfo.INSTANCE;
        } else if (logicalType instanceof SmallIntType) {
            return ShortFormatInfo.INSTANCE;
        } else if (logicalType instanceof IntType) {
            return IntFormatInfo.INSTANCE;
        } else if (logicalType instanceof BigIntType) {
            return LongFormatInfo.INSTANCE;
        } else if (logicalType instanceof FloatType) {
            return FloatFormatInfo.INSTANCE;
        } else if (logicalType instanceof DoubleType) {
            return DoubleFormatInfo.INSTANCE;
        } else if (logicalType instanceof DecimalType) {
            return DecimalFormatInfo.INSTANCE;
        } else if (logicalType instanceof DateType) {
            return new DateFormatInfo();
        } else if (logicalType instanceof TimeType) {
            return new TimeFormatInfo();
        } else if (logicalType instanceof TimestampType) {
            return new TimestampFormatInfo();
        } else if (logicalType instanceof LocalZonedTimestampType) {
            return new LocalZonedTimestampFormatInfo();
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            LogicalType elementType = arrayType.getElementType();

            FormatInfo elementFormatInfo = deriveFormatInfo(elementType);

            return new ArrayFormatInfo(elementFormatInfo);
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();

            FormatInfo keyFormatInfo = deriveFormatInfo(keyType);
            FormatInfo valueFormatInfo = deriveFormatInfo(valueType);

            return new MapFormatInfo(keyFormatInfo, valueFormatInfo);
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            List<RowType.RowField> rowFields = rowType.getFields();

            String[] fieldNames = new String[rowFields.size()];
            FormatInfo[] fieldFormatInfos = new FormatInfo[rowFields.size()];

            for (int i = 0; i < rowFields.size(); ++i) {
                RowType.RowField rowField = rowFields.get(i);

                fieldNames[i] = rowField.getName();
                fieldFormatInfos[i] = deriveFormatInfo(rowField.getType());
            }

            return new RowFormatInfo(fieldNames, fieldFormatInfos);
        } else if (logicalType instanceof BinaryType) {
            return BinaryFormatInfo.INSTANCE;
        } else if (logicalType instanceof VarBinaryType) {
            return VarBinaryFormatInfo.INSTANCE;
        } else if (logicalType instanceof NullType) {
            return NullFormatInfo.INSTANCE;
        } else {
            throw new IllegalArgumentException(String.format("not found logicalType %s",
                    logicalType == null ? "null" : logicalType.toString()));
        }
    }

    /**
     * Derive the LogicalType for the given FormatInfo.
     */
    public static LogicalType deriveLogicalType(FormatInfo formatInfo) {
        if (formatInfo instanceof StringFormatInfo) {
            return new VarCharType(VarCharType.MAX_LENGTH);
        } else if (formatInfo instanceof VarCharFormatInfo) {
            return new VarCharType(((VarCharFormatInfo) formatInfo).getLength());
        } else if (formatInfo instanceof BooleanFormatInfo) {
            return new BooleanType();
        } else if (formatInfo instanceof ByteFormatInfo) {
            return new TinyIntType();
        } else if (formatInfo instanceof ShortFormatInfo) {
            return new SmallIntType();
        } else if (formatInfo instanceof IntFormatInfo) {
            return new IntType();
        } else if (formatInfo instanceof LongFormatInfo) {
            return new BigIntType();
        } else if (formatInfo instanceof FloatFormatInfo) {
            return new FloatType();
        } else if (formatInfo instanceof DoubleFormatInfo) {
            return new DoubleType();
        } else if (formatInfo instanceof DecimalFormatInfo) {
            DecimalFormatInfo decimalFormatInfo = (DecimalFormatInfo) formatInfo;
            return new DecimalType(decimalFormatInfo.getPrecision(), decimalFormatInfo.getScale());
        } else if (formatInfo instanceof TimeFormatInfo) {
            return new TimeType(((TimeFormatInfo) formatInfo).getPrecision());
        } else if (formatInfo instanceof DateFormatInfo) {
            return new DateType();
        } else if (formatInfo instanceof TimestampFormatInfo) {
            return new TimestampType(((TimestampFormatInfo) formatInfo).getPrecision());
        } else if (formatInfo instanceof LocalZonedTimestampFormatInfo) {
            return new LocalZonedTimestampType(((LocalZonedTimestampFormatInfo) formatInfo).getPrecision());
        } else if (formatInfo instanceof ArrayFormatInfo) {
            FormatInfo elementFormatInfo = ((ArrayFormatInfo) formatInfo).getElementFormatInfo();
            return new ArrayType(deriveLogicalType(elementFormatInfo));
        } else if (formatInfo instanceof MapFormatInfo) {
            MapFormatInfo mapFormatInfo = (MapFormatInfo) formatInfo;
            FormatInfo keyFormatInfo = mapFormatInfo.getKeyFormatInfo();
            FormatInfo valueFormatInfo = mapFormatInfo.getValueFormatInfo();
            return new MapType(deriveLogicalType(keyFormatInfo), deriveLogicalType(valueFormatInfo));
        } else if (formatInfo instanceof RowFormatInfo) {
            RowFormatInfo rowFormatInfo = (RowFormatInfo) formatInfo;
            FormatInfo[] formatInfos = rowFormatInfo.getFieldFormatInfos();
            int formatInfosSize = formatInfos.length;
            LogicalType[] logicalTypes = new LogicalType[formatInfosSize];

            for (int i = 0; i < formatInfosSize; ++i) {
                logicalTypes[i] = deriveLogicalType(formatInfos[i]);
            }
            return RowType.of(logicalTypes, rowFormatInfo.getFieldNames());
        } else if (formatInfo instanceof BinaryFormatInfo) {
            BinaryFormatInfo binaryFormatInfo = (BinaryFormatInfo) formatInfo;
            return new BinaryType(binaryFormatInfo.getLength());
        } else if (formatInfo instanceof VarBinaryFormatInfo) {
            VarBinaryFormatInfo varBinaryFormatInfo = (VarBinaryFormatInfo) formatInfo;
            return new VarBinaryType(varBinaryFormatInfo.getLength());
        } else if (formatInfo instanceof NullFormatInfo) {
            return new NullType();
        } else {
            throw new IllegalArgumentException(String.format("not found formatInfo %s",
                    formatInfo == null ? "null" : formatInfo.toString()));
        }
    }

    /**
     * Returns the type represented by the given format.
     *
     * @param typeInfo The type information.
     * @return The type represented by the given format.
     */
    public static TypeInformation<?> getType(TypeInfo typeInfo) {
        if (typeInfo instanceof StringTypeInfo) {
            return Types.STRING;
        } else if (typeInfo instanceof BooleanTypeInfo) {
            return Types.BOOLEAN;
        } else if (typeInfo instanceof ByteTypeInfo) {
            return Types.BYTE;
        } else if (typeInfo instanceof ShortTypeInfo) {
            return Types.SHORT;
        } else if (typeInfo instanceof IntTypeInfo) {
            return Types.INT;
        } else if (typeInfo instanceof LongTypeInfo) {
            return Types.LONG;
        } else if (typeInfo instanceof FloatTypeInfo) {
            return Types.FLOAT;
        } else if (typeInfo instanceof DoubleTypeInfo) {
            return Types.DOUBLE;
        } else if (typeInfo instanceof DecimalTypeInfo) {
            return Types.BIG_DEC;
        } else if (typeInfo instanceof DateTypeInfo) {
            return Types.SQL_DATE;
        } else if (typeInfo instanceof TimeTypeInfo) {
            return Types.SQL_TIME;
        } else if (typeInfo instanceof TimestampTypeInfo) {
            return Types.SQL_TIMESTAMP;
        } else if (typeInfo instanceof LocalZonedTimestampTypeInfo) {
            return Types.LOCAL_DATE_TIME;
        } else if (typeInfo instanceof BinaryTypeInfo) {
            return Types.PRIMITIVE_ARRAY(Types.BYTE);
        } else if (typeInfo instanceof ArrayTypeInfo) {
            ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
            TypeInfo elementTypeInfo =
                    arrayTypeInfo.getElementTypeInfo();
            TypeInformation<?> elementType = getType(elementTypeInfo);

            return Types.OBJECT_ARRAY(elementType);
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            TypeInfo keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
            TypeInfo valueTypeInfo = mapTypeInfo.getValueTypeInfo();

            TypeInformation<?> keyType = getType(keyTypeInfo);
            TypeInformation<?> valueType = getType(valueTypeInfo);

            return Types.MAP(keyType, valueType);
        } else if (typeInfo instanceof RowTypeInfo) {
            RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
            String[] fieldNames = rowTypeInfo.getFieldNames();
            TypeInfo[] fieldTypeInfos = rowTypeInfo.getFieldTypeInfos();

            TypeInformation<?>[] fieldTypes =
                    Arrays.stream(fieldTypeInfos)
                            .map(TableFormatUtils::getType)
                            .toArray(TypeInformation<?>[]::new);

            return Types.ROW_NAMED(fieldNames, fieldTypes);
        } else {
            throw new IllegalStateException("Unexpected type info " + typeInfo + ".");
        }
    }

    /**
     * Returns the Flink SQL type represented by the given format.
     * Please see {@link org.apache.flink.table.types.utils.TypeInfoDataTypeConverter} and
     * {@link org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter}.
     *
     * @param typeInfo The type information of flink formats.
     * @return The Flink SQL data type represented by the given format.
     */
    public static DataType getDataType(TypeInfo typeInfo) {
        if (typeInfo instanceof StringTypeInfo) {
            return DataTypes.STRING().bridgedTo(String.class);
        } else if (typeInfo instanceof BooleanTypeInfo) {
            return DataTypes.BOOLEAN().bridgedTo(Boolean.class);
        } else if (typeInfo instanceof ByteTypeInfo) {
            return DataTypes.TINYINT().bridgedTo(Byte.class);
        } else if (typeInfo instanceof ShortTypeInfo) {
            return DataTypes.SMALLINT().bridgedTo(Short.class);
        } else if (typeInfo instanceof IntTypeInfo) {
            return DataTypes.INT().bridgedTo(Integer.class);
        } else if (typeInfo instanceof LongTypeInfo) {
            return DataTypes.BIGINT().bridgedTo(Long.class);
        } else if (typeInfo instanceof FloatTypeInfo) {
            return DataTypes.FLOAT().bridgedTo(Float.class);
        } else if (typeInfo instanceof DoubleTypeInfo) {
            return DataTypes.DOUBLE().bridgedTo(Double.class);
        } else if (typeInfo instanceof DecimalTypeInfo) {
            return DataTypes.DECIMAL(38, 18).bridgedTo(BigDecimal.class);
        } else if (typeInfo instanceof DateTypeInfo) {
            return DataTypes.DATE().bridgedTo(java.sql.Date.class);
        } else if (typeInfo instanceof TimeTypeInfo) {
            return DataTypes.TIME(0).bridgedTo(java.sql.Time.class);
        } else if (typeInfo instanceof TimestampTypeInfo) {
            return DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);
        } else if (typeInfo instanceof ArrayTypeInfo) {
            ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
            TypeInfo elementTypeInfo =
                    arrayTypeInfo.getElementTypeInfo();
            DataType elementType = getDataType(elementTypeInfo);
            // copied from ObjectArrayTypeInfo#getInfoFor
            Class<?> arrayClass = Array.newInstance(elementType.getConversionClass(), 0).getClass();
            return DataTypes.ARRAY(elementType).bridgedTo(arrayClass);
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            TypeInfo keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
            TypeInfo valueTypeInfo = mapTypeInfo.getValueTypeInfo();

            DataType keyType = getDataType(keyTypeInfo);
            DataType valueType = getDataType(valueTypeInfo);

            return DataTypes.MAP(keyType, valueType).bridgedTo(Map.class);
        } else if (typeInfo instanceof RowTypeInfo) {
            RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
            String[] fieldNames = rowTypeInfo.getFieldNames();
            TypeInfo[] fieldTypeInfos = rowTypeInfo.getFieldTypeInfos();

            DataTypes.Field[] fields = IntStream.range(0, fieldNames.length)
                    .mapToObj(i -> {
                        DataType fieldType = getDataType(fieldTypeInfos[i]);
                        return DataTypes.FIELD(fieldNames[i], fieldType);
                    }).toArray(DataTypes.Field[]::new);

            return DataTypes.ROW(fields).bridgedTo(Row.class);
        } else {
            throw new IllegalStateException("Unexpected format.");
        }
    }

    /**
     * Returns the format defined in the given property.
     *
     * @param descriptorProperties The properties of the descriptor.
     * @return The basic row format defined in the descriptor.
     */
    public static RowFormatInfo deserializeRowFormatInfo(
            DescriptorProperties descriptorProperties) {
        try {
            String schema = descriptorProperties.getString(FORMAT_SCHEMA);

            FormatInfo formatInfo = FormatUtils.demarshall(schema);
            if (!(formatInfo instanceof RowFormatInfo)) {
                throw new IllegalStateException("Unexpected format type.");
            }

            return (RowFormatInfo) formatInfo;
        } catch (Exception e) {
            throw new ValidationException("The schema is invalid.", e);
        }
    }

    /**
     * Derives the format from the given schema.
     *
     * @param descriptorProperties The properties of the descriptor.
     * @return The format derived from the schema in the descriptor.
     */
    public static RowFormatInfo deriveRowFormatInfo(
            DescriptorProperties descriptorProperties) {
        TableSchema tableSchema =
                deriveSchema(descriptorProperties.asMap());

        int numFields = tableSchema.getFieldCount();
        String[] fieldNames = tableSchema.getFieldNames();
        DataType[] fieldTypes = tableSchema.getFieldDataTypes();

        FormatInfo[] fieldFormatInfos = new FormatInfo[numFields];
        for (int i = 0; i < numFields; ++i) {
            LogicalType fieldType = fieldTypes[i].getLogicalType();
            fieldFormatInfos[i] = deriveFormatInfo(fieldType);
        }

        return new RowFormatInfo(fieldNames, fieldFormatInfos);
    }

    /**
     * Returns the schema in the properties.
     *
     * @param descriptorProperties The properties of the descriptor.
     * @return The schema in the properties.
     */
    public static RowFormatInfo getRowFormatInfo(
            DescriptorProperties descriptorProperties) {
        if (descriptorProperties.containsKey(FORMAT_SCHEMA)) {
            return deserializeRowFormatInfo(descriptorProperties);
        } else {
            return deriveRowFormatInfo(descriptorProperties);
        }
    }

    /**
     * Projects the given schema.
     *
     * @param rowFormatInfo The schema to be projected.
     * @return The projected schema in the properties.
     */
    public static RowFormatInfo projectRowFormatInfo(
            RowFormatInfo rowFormatInfo,
            int[] fields) {
        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();

        String[] projectedFieldNames = new String[fields.length];
        FormatInfo[] projectedFieldFormatInfos = new FormatInfo[fields.length];

        for (int i = 0; i < fields.length; ++i) {
            projectedFieldNames[i] = fieldNames[fields[i]];
            projectedFieldFormatInfos[i] = fieldFormatInfos[fields[i]];
        }

        return new RowFormatInfo(projectedFieldNames, projectedFieldFormatInfos);
    }

    /**
     * Validates the schema in the descriptor.
     *
     * @param descriptorProperties The properties of the descriptor.
     */
    public static void validateSchema(DescriptorProperties descriptorProperties) {
        final boolean defineSchema = descriptorProperties.containsKey(FORMAT_SCHEMA);
        final boolean deriveSchema = descriptorProperties.containsKey(FORMAT_DERIVE_SCHEMA);

        if (defineSchema && deriveSchema) {
            throw new ValidationException("Format cannot define a schema and "
                    + "derive from the table's schema at the same time.");
        } else if (defineSchema) {
            descriptorProperties.validateString(FORMAT_SCHEMA, false);
        } else if (deriveSchema) {
            descriptorProperties.validateBoolean(FORMAT_DERIVE_SCHEMA, false);
        } else {
            throw new ValidationException("A definition of a schema or "
                    + "derivation from the table's schema is required.");
        }
    }

    public static TableSchema deriveSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);

        final TableSchema.Builder builder = TableSchema.builder();

        final TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            final TableColumn tableColumn = tableSchema.getTableColumns().get(i);
            final String fieldName = tableColumn.getName();
            final DataType dataType = tableColumn.getType();
            if (!tableColumn.isPhysical()) {
                // skip non-physical columns
                continue;
            }
            final boolean isProctime =
                    descriptorProperties
                            .getOptionalBoolean(SCHEMA + '.' + i + '.' + SCHEMA_PROCTIME)
                            .orElse(false);
            final String timestampKey = SCHEMA + '.' + i + '.' + ROWTIME_TIMESTAMPS_TYPE;
            final boolean isRowtime = descriptorProperties.containsKey(timestampKey);
            if (!isProctime && !isRowtime) {
                // check for aliasing
                final String aliasName =
                        descriptorProperties
                                .getOptionalString(SCHEMA + '.' + i + '.' + SCHEMA_FROM)
                                .orElse(fieldName);
                builder.field(aliasName, dataType);
            }
            // only use the rowtime attribute if it references a field
            else if (isRowtime
                    && descriptorProperties.isValue(
                            timestampKey, ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_FIELD)) {
                final String aliasName =
                        descriptorProperties.getString(
                                SCHEMA + '.' + i + '.' + ROWTIME_TIMESTAMPS_FROM);
                builder.field(aliasName, dataType);
            }
        }

        return builder.build();
    }

    /**
     * Deserializes the basic field.
     */
    public static Object deserializeBasicField(
            String fieldName,
            FormatInfo fieldFormatInfo,
            String fieldText,
            String nullLiteral,
            FailureHandler failureHandler) throws Exception {
        checkState(fieldFormatInfo instanceof BasicFormatInfo);

        if (fieldText == null) {
            return null;
        }

        if (nullLiteral == null) {
            if (fieldText.isEmpty()) {
                if (fieldFormatInfo instanceof StringFormatInfo) {
                    return "";
                } else {
                    return null;
                }
            }
        } else {
            if (fieldText.equals(nullLiteral)) {
                return null;
            }
        }

        try {
            return ((BasicFormatInfo<?>) fieldFormatInfo).deserialize(fieldText);
        } catch (Exception e) {
            LOG.warn("Could not properly deserialize the " + "text "
                    + fieldText + " for field " + fieldName + ".", e);
            if (failureHandler != null) {
                failureHandler.onConvertingFieldFailure(fieldName, fieldText, fieldFormatInfo, e);
            }
        }
        return null;
    }

    /**
     * Serializes the basic field.
     */
    @SuppressWarnings("unchecked")
    public static String serializeBasicField(
            String fieldName,
            FormatInfo fieldFormatInfo,
            Object field,
            String nullLiteral) {
        checkState(fieldFormatInfo instanceof BasicFormatInfo);

        if (field == null) {
            return nullLiteral == null ? "" : nullLiteral;
        }

        try {
            return ((BasicFormatInfo<Object>) fieldFormatInfo).serialize(field);
        } catch (Exception e) {
            throw new RuntimeException("Could not properly serialize the "
                    + "value " + field + " for field " + fieldName + ".", e);
        }
    }

    public static RowFormatInfo deriveRowFormatInfo(
            RowType rowType) {
        List<RowType.RowField> fields = rowType.getFields();
        FormatInfo[] fieldFormatInfos = new FormatInfo[fields.size()];
        for (int i = 0; i < fields.size(); ++i) {
            LogicalType fieldType = fields.get(i).getType();
            fieldFormatInfos[i] = TableFormatUtils.deriveFormatInfo(fieldType);
        }

        return new RowFormatInfo(rowType.getFieldNames().toArray(new String[1]), fieldFormatInfos);
    }

    public static RowFormatInfo deriveRowFormatInfo(DataType dataType) {

        RowType rowType = (RowType) dataType.getLogicalType();
        int size = rowType.getFields().size();
        FormatInfo[] fieldFormatInfos = new FormatInfo[size];
        String[] fieldNames = new String[size];

        for (int i = 0; i < size; i++) {
            LogicalType fieldType = rowType.getTypeAt(i);
            fieldFormatInfos[i] = deriveFormatInfo(fieldType);
            fieldNames[i] = rowType.getFieldNames().get(i);
        }

        return new RowFormatInfo(fieldNames, fieldFormatInfos);
    }

    public static RowFormatInfo deserializeRowFormatInfo(String rowFormatInfoStr) {
        try {
            FormatInfo formatInfo = FormatUtils.demarshall(rowFormatInfoStr);
            if (!(formatInfo instanceof RowFormatInfo)) {
                throw new IllegalStateException("Unexpected format type.");
            }

            return (RowFormatInfo) formatInfo;
        } catch (Exception e) {
            throw new ValidationException("The schema is invalid.", e);
        }
    }

    public static void getValidateProperties(DescriptorProperties properties) {
        properties.validateString(TableFormatConstants.FORMAT_TYPE, false, 1);
        properties.validateString(FORMAT_PROPERTY_VERSION, true, 1);
        properties.validateString(TableFormatConstants.FORMAT_ESCAPE_CHARACTER, true, 1, 1);
    }
}
