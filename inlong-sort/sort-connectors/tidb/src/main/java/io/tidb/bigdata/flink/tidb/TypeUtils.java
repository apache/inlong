/*
 * Copyright 2020 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.tidb;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.tikv.common.types.MySQLType.TypeDatetime;
import static org.tikv.common.types.MySQLType.TypeTimestamp;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.tikv.common.types.MySQLType;
import org.tikv.common.types.StringType;

public class TypeUtils {

    public static final String TIMESTAMP_FORMAT_PREFIX = "tidb.timestamp-format.";

    // maintain compatibility with flink-1.11, 1.12, will remove this configuration in flink-1.14
    public static final String OLD_TIMESTAMP_FORMAT_PREFIX = "timestamp-format.";

    /**
     * a default mapping: TiKV DataType -> Flink DataType
     *
     * @param dataType TiKV DataType
     * @return Flink DataType
     */
    public static DataType getFlinkType(org.tikv.common.types.DataType dataType) {
        boolean unsigned = dataType.isUnsigned();
        int length = (int) dataType.getLength();
        switch (dataType.getType()) {
            case TypeBit:
                return DataTypes.BOOLEAN();
            case TypeTiny:
                return unsigned ? DataTypes.SMALLINT() : DataTypes.TINYINT();
            case TypeYear:
            case TypeShort:
                return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
            case TypeInt24:
            case TypeLong:
                return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
            case TypeLonglong:
                return unsigned ? DataTypes.DECIMAL(length, 0) : DataTypes.BIGINT();
            case TypeFloat:
                return DataTypes.FLOAT();
            case TypeDouble:
                return DataTypes.DOUBLE();
            case TypeNull:
                return DataTypes.NULL();
            case TypeDatetime:
            case TypeTimestamp:
                return DataTypes.TIMESTAMP();
            case TypeDate:
            case TypeNewDate:
                return DataTypes.DATE();
            case TypeDuration:
                return DataTypes.TIME();
            case TypeTinyBlob:
            case TypeMediumBlob:
            case TypeLongBlob:
            case TypeBlob:
            case TypeVarString:
            case TypeString:
            case TypeVarchar:
                if (dataType instanceof StringType) {
                    return DataTypes.STRING();
                }
                return DataTypes.BYTES();
            case TypeJSON:
            case TypeEnum:
            case TypeSet:
                return DataTypes.STRING();
            case TypeDecimal:
            case TypeNewDecimal:
                return DataTypes.DECIMAL(length, dataType.getDecimal());
            case TypeGeometry:
            default:
                throw new IllegalArgumentException(
                        format("can not get flink datatype by tikv type: %s", dataType));
        }
    }

    /**
     * transform TiKV java object to Flink java object by given Flink Datatype
     *
     * @param object TiKV java object
     * @param flinkType Flink datatype
     * @param tidbType TiDB datatype
     */
    public static Optional<Object> getObjectWithDataType(@Nullable Object object, DataType flinkType,
            org.tikv.common.types.DataType tidbType, @NotNull DateTimeFormatter formatter) {
        if (object == null) {
            return Optional.empty();
        }
        Class<?> conversionClass = flinkType.getConversionClass();
        if (flinkType.getConversionClass() == object.getClass()) {
            return Optional.of(object);
        }
        MySQLType mySqlType = tidbType.getType();
        switch (conversionClass.getSimpleName()) {
            case "String":
                if (object instanceof byte[]) {
                    object = new String((byte[]) object);
                } else if (object instanceof Timestamp) {
                    Timestamp timestamp = (Timestamp) object;
                    object = timestamp.toLocalDateTime().format(formatter);
                } else if (object instanceof Long
                        && (mySqlType == TypeTimestamp || mySqlType == TypeDatetime)) {
                    // covert tidb timestamp to flink string
                    object = new Timestamp(((long) object) / 1000).toLocalDateTime().format(formatter);
                } else {
                    object = object.toString();
                }
                break;
            case "Integer":
                object = (int) (long) getObjectWithDataType(object, DataTypes.BIGINT(), tidbType, formatter)
                        .get();
                break;
            case "Long":
                if (object instanceof LocalDate) {
                    object = ((LocalDate) object).toEpochDay();
                } else if (object instanceof LocalDateTime) {
                    object = Timestamp.valueOf(((LocalDateTime) object)).getTime();
                } else if (object instanceof LocalTime) {
                    object = ((LocalTime) object).toNanoOfDay();
                }
                break;
            case "LocalDate":
                if (object instanceof Date) {
                    object = ((Date) object).toLocalDate();
                } else if (object instanceof String) {
                    object = LocalDate.parse((String) object);
                } else if (object instanceof Long || object instanceof Integer) {
                    object = LocalDate.ofEpochDay(Long.parseLong(object.toString()));
                }
                break;
            case "LocalDateTime":
                if (object instanceof Timestamp) {
                    object = ((Timestamp) object).toLocalDateTime();
                } else if (object instanceof String) {
                    // convert flink string to timestamp
                    object = LocalDateTime.parse((String) object, formatter);
                } else if (object instanceof Long) {
                    object = new Timestamp(((Long) object) / 1000).toLocalDateTime();
                }
                break;
            case "LocalTime":
                if (object instanceof Long || object instanceof Integer) {
                    object = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()));
                }
                break;
            default:
                object = ConvertUtils.convert(object, conversionClass);
        }
        return Optional.of(object);
    }

    /**
     * transform Row to GenericRowData
     */
    public static Optional<GenericRowData> toRowData(Row row) {
        if (row == null) {
            return Optional.empty();
        }
        GenericRowData rowData = new GenericRowData(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            rowData.setField(i, toRowDataType(row.getField(i)));
        }
        return Optional.of(rowData);
    }

    /**
     * transform Row type to GenericRowData type
     */
    public static Object toRowDataType(Object object) {
        Object result = object;
        if (object == null) {
            return null;
        }
        switch (object.getClass().getSimpleName()) {
            case "String":
                result = StringData.fromString(object.toString());
                break;
            case "BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) object;
                result = DecimalData
                        .fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
                break;
            case "LocalDate":
                LocalDate localDate = (LocalDate) object;
                result = (int) localDate.toEpochDay();
                break;
            case "LocalDateTime":
                result = TimestampData.fromLocalDateTime((LocalDateTime) object);
                break;
            case "LocalTime":
                LocalTime localTime = (LocalTime) object;
                result = (int) (localTime.toNanoOfDay() / (1000 * 1000));
                break;
            default:
                // pass code style
                break;
        }
        return result;
    }

    /**
     * extracts the DateTimeFormatter of table field name according to
     * the configuration in properties, which will used in conversion of
     * tikv date type to flink date type.
     *
     * @param fieldNames table field names
     * @param confProps tidb-connector configuration
     * @param downwardCompatible whether to be compatible with old configurations
     * @return DateTimeFormatter DateTimeFormatter Objects
     */
    @Nonnull
    public static DateTimeFormatter[] extractDateTimeFormatter(
            String[] fieldNames,
            @Nonnull Map<String, String> confProps,
            boolean downwardCompatible) {
        if (fieldNames == null) {
            return new DateTimeFormatter[0];
        }
        assert (confProps != null);
        if (downwardCompatible) {
            return Arrays.stream(fieldNames)
                    .map(fieldName -> Optional
                            .ofNullable(confProps.get(TIMESTAMP_FORMAT_PREFIX + fieldName))
                            .orElseGet(() -> confProps.get(OLD_TIMESTAMP_FORMAT_PREFIX + fieldName)))
                    .map(pattern -> pattern == null ? ISO_LOCAL_DATE : DateTimeFormatter.ofPattern(pattern))
                    .toArray(DateTimeFormatter[]::new);
        } else {
            return Arrays.stream(fieldNames)
                    .map(fieldName -> {
                        String pattern = confProps.get(TIMESTAMP_FORMAT_PREFIX + fieldName);
                        return pattern == null ? ISO_LOCAL_DATE : DateTimeFormatter.ofPattern(pattern);
                    }).toArray(DateTimeFormatter[]::new);
        }
    }

}
