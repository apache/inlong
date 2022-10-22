/*
 * Copyright 2021 TiDB Project Authors.
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

package io.tidb.bigdata.flink.format.cdc;

import io.tidb.bigdata.cdc.RowColumn;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

public final class RowColumnConverters {

  private static Converter createConverterUnsafe(final LogicalType type) {
    switch (type.getTypeRoot()) {
      case NULL:
        return RowColumn::asNull;
      case BOOLEAN:
        return RowColumn::asBoolean;
      case TINYINT:
        return RowColumn::asTinyInt;
      case SMALLINT:
        return RowColumn::asSmallInt;
      case INTEGER:
        // FALL THROUGH
      case INTERVAL_YEAR_MONTH:
        return RowColumn::asInt;
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return RowColumn::asBigInt;
      case DATE:
        return column -> (int) column.asDate().toEpochDay();
      case TIME_WITHOUT_TIME_ZONE:
        return column -> column.asTime().get(ChronoField.MILLI_OF_DAY);
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return column -> {
          final TemporalAccessor parsedTimestamp = column.asDateTime();
          final LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
          final LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
          return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
        };
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return column -> {
          final TemporalAccessor parsedTimestamp = column.asDateTime();
          final LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
          final LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
          return TimestampData.fromInstant(LocalDateTime.of(localDate, localTime)
              .toInstant(ZoneOffset.UTC));
        };
      case FLOAT:
        return RowColumn::asFloat;
      case DOUBLE:
        return RowColumn::asDouble;
      case CHAR:
        return column -> StringData.fromString(column.asChar());
      case VARCHAR:
        return column -> StringData.fromString(column.asVarchar());
      case BINARY:
        return RowColumn::asBinary;
      case VARBINARY:
        return RowColumn::asVarbinary;
      case DECIMAL:
        return createDecimalConverter((DecimalType) type);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  public static Converter createConverter(final LogicalType type) {
    final Converter converter = createConverterUnsafe(type);
    return column -> {
      if (column == null) {
        return null;
      }
      try {
        return converter.convert(column);
      } catch (Exception ex) {
        throw new IllegalStateException("Failed to convert cdc data for column:" + column.getName(),
            ex);
      }
    };
  }

  private static Converter createDecimalConverter(final DecimalType decimalType) {
    final int precision = decimalType.getPrecision();
    final int scale = decimalType.getScale();
    return column -> DecimalData.fromBigDecimal(column.asDecimal(), precision, scale);
  }

  @FunctionalInterface
  public interface Converter extends Serializable {

    Object convert(final RowColumn column);
  }
}
