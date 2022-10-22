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

package io.tidb.bigdata.flink.connector.source;

import static org.tikv.common.types.MySQLType.TypeDatetime;
import static org.tikv.common.types.MySQLType.TypeTimestamp;

import com.google.common.collect.ImmutableMap;
import io.tidb.bigdata.flink.tidb.TypeUtils;
import io.tidb.bigdata.tidb.RecordCursorInternal;
import java.io.Serializable;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.common.types.MySQLType;

public class TiDBSchemaAdapter implements Serializable {
  private final DataType physicalDataType;
  private final int physicalFieldCount;
  private final String[] physicalFieldNames;
  private final DataType[] physicalFieldTypes;
  private final int producedFieldCount;
  private final TiDBMetadata[] metadata;
  private final TypeInformation<RowData> typeInfo;
  private final Map<String, String> properties;
  private transient DateTimeFormatter[] dateTimeFormatters;

  public TiDBSchemaAdapter(ResolvedCatalogTable table,
      Function<DataType, TypeInformation<RowData>> typeInfoFactory,
      TiDBMetadata[] metadata, int[] projectedFields) {
    ResolvedSchema schema = table.getResolvedSchema();

    Field[] physicalFields = schema.getColumns()
        .stream().filter(Column::isPhysical).map(c ->
            DataTypes.FIELD(c.getName(), DataTypeUtils.removeTimeAttribute(c.getDataType()))
        ).toArray(Field[]::new);

    if (projectedFields != null) {
      Field[] projectedPhysicalFields = new Field[projectedFields.length];
      for (int idx = 0; idx < projectedFields.length; ++idx) {
        projectedPhysicalFields[idx] = physicalFields[projectedFields[idx]];
      }
      physicalFields = projectedPhysicalFields;
    }

    this.physicalDataType = DataTypes.ROW(physicalFields).notNull();
    this.physicalFieldNames = Arrays.stream(physicalFields)
        .map(Field::getName).toArray(String[]::new);
    this.physicalFieldTypes = Arrays.stream(physicalFields)
        .map(Field::getDataType).toArray(DataType[]::new);
    this.physicalFieldCount = physicalFieldNames.length;

    final DataType producedDataType;
    if (metadata != null) {
      producedDataType = DataTypeUtils.appendRowFields(physicalDataType,
          Arrays.stream(metadata).map(TiDBMetadata::toField).collect(Collectors.toList()));
      producedFieldCount = physicalFieldCount + metadata.length;
    } else {
      producedDataType = physicalDataType;
      producedFieldCount = physicalFieldCount;
    }
    this.typeInfo = typeInfoFactory.apply(producedDataType);
    this.metadata = metadata;
    this.properties = table.getOptions();
  }

  public TiDBMetadata[] getMetadata() {
    return metadata;
  }

  private Object[] makeRow(final TiTimestamp timestamp) {
    int metaIndex = physicalFieldCount;
    Object[] objects = new Object[producedFieldCount];
    if (metadata != null) {
      for (TiDBMetadata meta : metadata) {
        objects[metaIndex++] = meta.extract(timestamp);
      }
    }
    return objects;
  }

  public String[] getPhysicalFieldNames() {
    return physicalFieldNames;
  }

  public GenericRowData convert(final TiTimestamp timestamp, RecordCursorInternal cursor) {
    Object[] objects = makeRow(timestamp);
    for (int idx = 0; idx < physicalFieldCount; idx++) {
      objects[idx] = toRowDataType(
          getObjectWithDataType(cursor.getObject(idx), physicalFieldTypes[idx],
              cursor.getType(idx), dateTimeFormatters[idx]).orElse(null));
    }
    return GenericRowData.ofKind(RowKind.INSERT, objects);
  }

  // These two methods were copied from flink-base as some interfaces changed in 1.13 made
  // it very hard to reuse code in flink-base
  private static Object stringToFlink(Object object) {
    return StringData.fromString(object.toString());
  }

  private static Object bigDecimalToFlink(Object object) {
    BigDecimal bigDecimal = (BigDecimal) object;
    return DecimalData.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
  }

  private static Object localDateToFlink(Object object) {
    LocalDate localDate = (LocalDate) object;
    return (int) localDate.toEpochDay();
  }

  private static Object localDateTimeToFlink(Object object) {
    return TimestampData.fromLocalDateTime((LocalDateTime) object);
  }

  private static Object localTimeToFlink(Object object) {
    LocalTime localTime = (LocalTime) object;
    return (int) (localTime.toNanoOfDay() / (1000 * 1000));
  }

  public static Map<Class<?>, Function<Object, Object>> ROW_DATA_CONVERTERS =
      ImmutableMap.of(
          String.class, TiDBSchemaAdapter::stringToFlink,
          BigDecimal.class, TiDBSchemaAdapter::bigDecimalToFlink,
          LocalDate.class, TiDBSchemaAdapter::localDateToFlink,
          LocalDateTime.class, TiDBSchemaAdapter::localDateTimeToFlink,
          LocalTime.class, TiDBSchemaAdapter::localTimeToFlink
      );

  /**
   * transform Row type to RowData type
   */
  public static Object toRowDataType(Object object) {
    if (object == null) {
      return null;
    }

    Class<?> clazz = object.getClass();
    if (!ROW_DATA_CONVERTERS.containsKey(clazz)) {
      return object;
    } else {
      return ROW_DATA_CONVERTERS.get(clazz).apply(object);
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
        object = (int) (long)
            getObjectWithDataType(object, DataTypes.BIGINT(), tidbType, formatter)
                .orElseThrow(() -> new IllegalArgumentException("Failed to convert integer"));
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

  public DataType getPhysicalDataType() {
    return physicalDataType;
  }

  public TypeInformation<RowData> getProducedType() {
    return typeInfo;
  }

  public void open() {
    this.dateTimeFormatters = TypeUtils.extractDateTimeFormatter(
        physicalFieldNames, properties, true);
  }

}
