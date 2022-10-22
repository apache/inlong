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

package io.tidb.bigdata.cdc;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class RowColumn {

  /**
   * Formatter for SQL string representation of a time value.
   */
  static final DateTimeFormatter SQL_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter();
  /**
   * Formatter for SQL string representation of a timestamp value (without UTC timezone).
   */
  static final DateTimeFormatter SQL_TIMESTAMP_FORMAT =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral(' ')
          .append(SQL_TIME_FORMAT)
          .toFormatter();
  private static final short MYSQL_TYPE_DECIMAL = 0;
  private static final short MYSQL_TYPE_TINY = 1;
  private static final short MYSQL_TYPE_SHORT = 2;
  private static final short MYSQL_TYPE_LONG = 3;
  private static final short MYSQL_TYPE_FLOAT = 4;
  private static final short MYSQL_TYPE_DOUBLE = 5;
  private static final short MYSQL_TYPE_NULL = 6;
  private static final short MYSQL_TYPE_TIMESTAMP = 7;
  private static final short MYSQL_TYPE_LONGLONG = 8;
  private static final short MYSQL_TYPE_INT24 = 9;
  private static final short MYSQL_TYPE_DATE = 10;
  /*
   * private static final short MYSQL_TYPE_DURATION original name was private static final
   * short MYSQL_TYPE_Time, renamed to private static final short MYSQL_TYPE_Duration to
   * resolve the conflict with Go type Time.
   */
  private static final short MYSQL_TYPE_DURATION = 11;
  private static final short MYSQL_TYPE_DATETIME = 12;
  private static final short MYSQL_TYPE_YEAR = 13;
  private static final short MYSQL_TYPE_NEWDATE = 14;
  private static final short MYSQL_TYPE_VARCHAR = 15;
  private static final short MYSQL_TYPE_BIT = 16;
  private static final short MYSQL_TYPE_JSON = 0xf5;
  private static final short MYSQL_TYPE_NEWDECIMAL = 0xf6;
  private static final short MYSQL_TYPE_ENUM = 0xf7;
  private static final short MYSQL_TYPE_SET = 0xf8;
  private static final short MYSQL_TYPE_TINYBLOB = 0xf9;
  private static final short MYSQL_TYPE_MEDIUMBLOB = 0xfa;
  private static final short MYSQL_TYPE_LONGBLOB = 0xfb;
  private static final short MYSQL_TYPE_BLOB = 0xfc;
  private static final short MYSQL_TYPE_VARSTRING = 0xfd;
  private static final short MYSQL_TYPE_STRING = 0xfe;
  private static final short MYSQL_TYPE_GEOMETRY = 0xff;
  private static final long FLAG_BINARY = 1 << 0;
  private static final long FLAG_HANDLE_KEY = 1 << 1;
  private static final long FLAG_GENERATED_COLUMN = 1 << 2;
  private static final long FLAG_PRIMARY_KEY = 1 << 3;
  private static final long FLAG_UNIQUE_KEY = 1 << 4;
  private static final long FLAG_MULTIPLE_KEY = 1 << 5;
  private static final long FLAG_NULLABLE = 1 << 6;
  private static final long FLAG_UNSIGNED = 1 << 7;
  private final String name; // Column name
  private final Object value; // Value
  private final boolean whereHandle; // Where Handle
  private final Type type;
  private final long flags;
  private Optional<Object> coerced; // Coerced value according to it's type

  public RowColumn(final String name, final Object value, final boolean whereHandle, final int type,
      final long flags) {
    this.name = name;
    this.whereHandle = whereHandle;
    this.type = Type.findByCode(type);
    this.value = value;
    this.flags = flags;
    this.coerced = Optional.empty();
  }

  private static LocalDate parseDate(final String str) {
    return ISO_LOCAL_DATE.parse(str).query(TemporalQueries.localDate());
  }

  private static LocalTime parseTime(final String str) {
    return SQL_TIME_FORMAT.parse(str).query(TemporalQueries.localTime());
  }

  public static Type getType(int type) {
    return Type.findByCode(type);
  }

  public Type getType() {
    return type;
  }

  public static boolean isBinary(long flags) {
    return (flags & FLAG_BINARY) == FLAG_BINARY;
  }

  public boolean isBinary() {
    return type.isBinary();
  }

  public static boolean isHandleKey(long flags) {
    return (flags & FLAG_HANDLE_KEY) == FLAG_HANDLE_KEY;
  }

  public static boolean isGeneratedColumn(long flags) {
    return (flags & FLAG_GENERATED_COLUMN) == FLAG_GENERATED_COLUMN;
  }

  public static boolean isPrimaryKey(long flags) {
    return (flags & FLAG_PRIMARY_KEY) == FLAG_PRIMARY_KEY;
  }

  public static boolean isUniqueKey(long flags) {
    return (flags & FLAG_UNIQUE_KEY) == FLAG_UNIQUE_KEY;
  }

  public static boolean isMultipleKey(long flags) {
    return (flags & FLAG_MULTIPLE_KEY) == FLAG_MULTIPLE_KEY;
  }

  public static boolean isNullable(long flags) {
    return (flags & FLAG_NULLABLE) == FLAG_NULLABLE;
  }

  public static boolean isUnsigned(long flags) {
    return (flags & FLAG_UNSIGNED) == FLAG_UNSIGNED;
  }

  public long getFlags() {
    return flags;
  }

  public String getName() {
    return name;
  }

  public boolean isWhereHandle() {
    return whereHandle;
  }

  public Object getOriginalValue() {
    return value;
  }

  public Object getValue() {
    if (!coerced.isPresent()) {
      coerced = Optional.ofNullable(type.coerce(value));
    }
    return coerced.get();
  }

  public Class getJavaType() {
    return type.getJavaType();
  }

  private String asStringInternal() {
    if (getJavaType().equals(String.class)) {
      if (getValue() instanceof byte[]) {
        return new String((byte[]) getValue(), StandardCharsets.UTF_8);
      }
      return (String) getValue();
    } else {
      return value.toString();
    }
  }

  private String asStringNullable() {
    if (value == null) {
      return null;
    }
    return asStringInternal();
  }

  private byte[] asBinaryInternal() {
    if (getJavaType().equals(byte[].class)) {
      return (byte[]) getValue();
    } else {
      return asStringInternal().getBytes(StandardCharsets.UTF_8);
    }
  }

  private byte[] asBinaryNullable() {
    if (value == null) {
      return null;
    }
    return asBinaryInternal();
  }

  private Integer asIntegerInternal() {
    if (getJavaType().equals(Integer.class)) {
      return (Integer) getValue();
    } else {
      return Integer.parseInt(asStringInternal());
    }
  }

  private Integer asIntegerNullable() {
    if (value == null) {
      return null;
    }
    return asIntegerInternal();
  }

  @SuppressWarnings("unchecked")
  private <T> T safeAsType(Supplier<Boolean> test, Function<String, T> converter) {
    if (value == null) {
      return null;
    }
    if (test.get()) {
      return (T) getValue();
    } else {
      return converter.apply(asStringInternal());
    }
  }

  public boolean isBoolean() {
    return type.isBoolean();
  }

  public Boolean asBoolean() {
    return safeAsType(this::isBoolean, Boolean::parseBoolean);
  }

  public boolean isTinyInt() {
    return type.isTinyInt();
  }

  public Byte asTinyInt() {
    return safeAsType(this::isTinyInt, Byte::parseByte);
  }

  public boolean isSmallInt() {
    return type.isSmallInt();
  }

  public Short asSmallInt() {
    return safeAsType(this::isSmallInt, Short::parseShort);
  }

  public boolean isInt() {
    return type.isInt();
  }

  public Integer asInt() {
    return asIntegerNullable();
  }

  public boolean isFloat() {
    return type.isFloat();
  }

  public Float asFloat() {
    return safeAsType(this::isFloat, Float::parseFloat);
  }

  public boolean isDouble() {
    return type.isDouble();
  }

  public Double asDouble() {
    return safeAsType(this::isDouble, Double::parseDouble);
  }

  public boolean isNull() {
    return type.isNull();
  }

  public Object asNull() {
    return null;
  }

  public boolean isTimestamp() {
    return type.isTimestamp();
  }

  public TemporalAccessor asTimestamp() {
    return safeAsType(this::isTimestamp, SQL_TIMESTAMP_FORMAT::parse);
  }

  public boolean isBigInt() {
    return type.isBigInt();
  }

  public Long asBigInt() {
    return safeAsType(this::isBigInt, Long::parseLong);
  }

  public boolean isMediumInt() {
    return type.isMediumInt();
  }

  public Integer asMediumInt() {
    return asIntegerNullable();
  }

  public boolean isDate() {
    return type.isDate();
  }

  public LocalDate asDate() {
    return safeAsType(this::isDate, RowColumn::parseDate);
  }

  public boolean isTime() {
    return type.isTime();
  }

  public LocalTime asTime() {
    return safeAsType(this::isTime, RowColumn::parseTime);
  }

  public boolean isDateTime() {
    return type.isDateTime();
  }

  public TemporalAccessor asDateTime() {
    return safeAsType(this::isDateTime, SQL_TIMESTAMP_FORMAT::parse);
  }

  public boolean isYear() {
    return type.isYear();
  }

  public Short asYear() {
    return safeAsType(this::isYear, Short::parseShort);
  }

  public boolean isVarchar() {
    return type.isVarchar();
  }

  public String asVarchar() {
    return asStringNullable();
  }

  public boolean isVarbinary() {
    return type.isVarbinary();
  }

  public byte[] asVarbinary() {
    return asBinaryNullable();
  }

  public boolean isBit() {
    return type.isBit();
  }

  public Long asBit() {
    return safeAsType(this::isBit, Long::parseLong);
  }

  public boolean isJson() {
    return type.isJson();
  }

  public String asJson() {
    return asStringNullable();
  }

  public boolean isDecimal() {
    return type.isDecimal();
  }

  public BigDecimal asDecimal() {
    return safeAsType(this::isDecimal, BigDecimal::new);
  }

  public boolean isEnum() {
    return type.isEnum();
  }

  public Integer asEnum() {
    return safeAsType(this::isEnum, Integer::parseInt);
  }

  public boolean isSet() {
    return type.isSet();
  }

  public Integer asSet() {
    return safeAsType(this::isSet, Integer::parseInt);
  }

  public boolean isTinyText() {
    return type.isTinyText();
  }

  public String asTinyText() {
    return asStringNullable();
  }

  public boolean isTinyBlob() {
    return type.isTinyBlob();
  }

  public byte[] asTinyBlob() {
    return asBinaryNullable();
  }

  public boolean isMediumText() {
    return type.isMediumText();
  }

  public String asMediumText() {
    return asStringNullable();
  }

  public boolean isMediumBlob() {
    return type.isMediumBlob();
  }

  public byte[] asMediumBlob() {
    return asBinaryNullable();
  }

  public boolean isLongText() {
    return type.isLongText();
  }

  public String asLongText() {
    return asStringNullable();
  }

  public boolean isLongBlob() {
    return type.isLongBlob();
  }

  public byte[] asLongBlob() {
    return asBinaryNullable();
  }

  public boolean isText() {
    return type.isText();
  }

  public String asText() {
    return asStringNullable();
  }

  public boolean isBlob() {
    return type.isBlob();
  }

  public byte[] asBlob() {
    return asBinaryNullable();
  }

  public boolean isChar() {
    return type.isChar();
  }

  public String asChar() {
    return asStringNullable();
  }

  public byte[] asBinary() {
    return asBinaryNullable();
  }

  public boolean isGeometry() {
    return type.isGeometry();
  }

  public Object asGeometry() {
    throw new IllegalStateException("Geometry is not supported at this time");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowColumn)) {
      return false;
    }

    final RowColumn other = (RowColumn) o;
    return Objects.equals(type, other.type)
        && Objects.equals(name, other.name)
        && Objects.equals(value, other.value)
        && Objects.equals(whereHandle, other.whereHandle)
        && Objects.equals(flags, other.flags);
  }

  public enum Type {
    TINYINT(MYSQL_TYPE_TINY, Byte.class, Type::numberToByte),
    BOOL(MYSQL_TYPE_TINY, Boolean.class),
    SMALLINT(MYSQL_TYPE_SHORT, Short.class, Type::numberToShort),
    INT(MYSQL_TYPE_LONG, Integer.class, Type::numberToInt),
    FLOAT(MYSQL_TYPE_FLOAT, Float.class, Type::numberToFloat),
    DOUBLE(MYSQL_TYPE_DOUBLE, Double.class, Type::numberToDouble),
    NULL(MYSQL_TYPE_NULL, Void.class, Type::toNull),
    TIMESTAMP(MYSQL_TYPE_TIMESTAMP, TemporalAccessor.class, Type::stringToTemporal),
    BIGINT(MYSQL_TYPE_LONGLONG, Long.class, Type::numberToLong),
    MEDIUMINT(MYSQL_TYPE_INT24, Integer.class, Type::numberToInt),
    DATE(MYSQL_TYPE_DATE, LocalDate.class, Type::stringToLocalDate),
    TIME(MYSQL_TYPE_DURATION, LocalTime.class, Type::stringToLocalTime),
    DATETIME(MYSQL_TYPE_DATETIME, TemporalAccessor.class, Type::stringToTemporal),
    YEAR(MYSQL_TYPE_YEAR, Short.class, Type::numberToShort),
    NEWDATE(MYSQL_TYPE_NEWDATE, TemporalAccessor.class, Type::stringToTemporal),
    VARCHAR(MYSQL_TYPE_VARCHAR, String.class, Type::bytesToString),
    BIT(MYSQL_TYPE_BIT, Long.class, Type::numberToLong),
    JSON(MYSQL_TYPE_JSON, String.class, Type::bytesToString),
    DECIMAL(MYSQL_TYPE_NEWDECIMAL, BigDecimal.class, Type::stringToDecimal),
    ENUM(MYSQL_TYPE_ENUM, Integer.class, Type::numberToInt),
    SET(MYSQL_TYPE_SET, Integer.class, Type::numberToInt),
    TINYTEXT(MYSQL_TYPE_TINYBLOB, String.class, Type::bytesToString),
    TINYBLOB(MYSQL_TYPE_TINYBLOB, byte[].class, Type::stringToBytes),
    MEDIUMTEXT(MYSQL_TYPE_MEDIUMBLOB, String.class),
    MEDIUMBLOB(MYSQL_TYPE_MEDIUMBLOB, byte[].class, Type::stringToBytes),
    LONGTEXT(MYSQL_TYPE_LONGBLOB, String.class, Type::bytesToString),
    LONGBLOB(MYSQL_TYPE_LONGBLOB, byte[].class, Type::stringToBytes),
    TEXT(MYSQL_TYPE_BLOB, String.class, Type::bytesToString),
    BLOB(MYSQL_TYPE_BLOB, byte[].class, Type::stringToBytes),
    VARBINARY(MYSQL_TYPE_VARSTRING, byte[].class, Type::stringToBytes),
    CHAR(MYSQL_TYPE_STRING, String.class, Type::bytesToString),
    BINARY(MYSQL_TYPE_STRING, byte[].class, Type::stringToBytes),
    GEOMETRY(MYSQL_TYPE_GEOMETRY, Void.class,
        Type::toNull); /* Geometry is not supported at this time */

    private static final Map<Integer, Type> byId = new HashMap<>();

    static {
      for (Type t : Type.values()) {
        if (byId.containsKey(t.code())) {
          // we use the first definition for those types sharing the same code
          continue;
        }
        byId.put(t.code(), t);
      }
      // 14 map to DATE as well
      byId.put(14, DATE);
    }

    private final int code;
    private final Class javaType;
    private final Function<Object, Object> coercion;

    Type(final int code, final Class javaType, final Function<Object, Object> coercion) {
      this.code = code;
      this.javaType = javaType;
      this.coercion = coercion;
    }

    Type(int code, Class javaType) {
      this(code, javaType, null);
    }

    static Type findByCode(final int code) {
      Type type = byId.get(code);
      if (type == null) {
        throw new IllegalArgumentException("Unknown type code: " + code);
      }
      return type;
    }

    private static Object numberToByte(final Object from) {
      return ((Number) from).byteValue();
    }

    private static Object numberToShort(final Object from) {
      return ((Number) from).shortValue();
    }

    private static Object numberToInt(final Object from) {
      return ((Number) from).intValue();
    }

    private static Object numberToLong(final Object from) {
      return ((Number) from).longValue();
    }

    private static Object numberToFloat(final Object from) {
      return ((Number) from).floatValue();
    }

    private static Object numberToDouble(final Object from) {
      return ((Number) from).doubleValue();
    }

    private static Object toNull(final Object from) {
      return null;
    }

    private static Object stringToTemporal(final Object from) {
      return SQL_TIMESTAMP_FORMAT.parse((String) from);
    }

    private static Object stringToDecimal(final Object from) {
      return new BigDecimal((String) from);
    }

    private static Object stringToLocalDate(final Object from) {
      return ISO_LOCAL_DATE.parse((String) from).query(TemporalQueries.localDate());
    }

    private static Object stringToLocalTime(final Object from) {
      return SQL_TIME_FORMAT.parse((String) from).query(TemporalQueries.localTime());
    }

    private static Object stringToBytes(final Object from) {
      return from.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static Object bytesToString(final Object from) {
      return new String((byte[]) from, StandardCharsets.UTF_8);
    }

    public Class getJavaType() {
      return javaType;
    }

    public int code() {
      return code;
    }

    public Object coerce(final Object from) {
      if (from == null || from.getClass().equals(javaType)) {
        return from;
      }
      if (coercion != null) {
        return coercion.apply(from);
      } else {
        return from;
      }
    }

    public boolean isBoolean() {
      return this.equals(BOOL);
    }

    public boolean isTinyInt() {
      return this.equals(TINYINT);
    }

    public boolean isSmallInt() {
      return this.equals(SMALLINT);
    }

    public boolean isInt() {
      return this.equals(INT);
    }

    public boolean isFloat() {
      return this.equals(FLOAT);
    }

    public boolean isDouble() {
      return this.equals(DOUBLE);
    }

    public boolean isNull() {
      return this.equals(NULL);
    }

    public boolean isTimestamp() {
      return this.equals(TIMESTAMP);
    }

    public boolean isBigInt() {
      return this.equals(BIGINT);
    }

    public boolean isMediumInt() {
      return this.equals(MEDIUMINT);
    }

    public boolean isDate() {
      return this.equals(DATE);
    }

    public boolean isTime() {
      return this.equals(TIME);
    }

    public boolean isDateTime() {
      return this.equals(DATETIME);
    }

    public boolean isYear() {
      return this.equals(YEAR);
    }

    public boolean isVarchar() {
      return this.equals(VARCHAR);
    }

    public boolean isVarbinary() {
      return this.equals(VARBINARY);
    }

    public boolean isBit() {
      return this.equals(BIT);
    }

    public boolean isJson() {
      return this.equals(JSON);
    }

    public boolean isDecimal() {
      return this.equals(DECIMAL);
    }

    public boolean isEnum() {
      return this.equals(ENUM);
    }

    public boolean isSet() {
      return this.equals(SET);
    }

    public boolean isTinyText() {
      return this.equals(TINYTEXT);
    }

    public boolean isTinyBlob() {
      return this.equals(TINYBLOB);
    }

    public boolean isMediumText() {
      return this.equals(MEDIUMTEXT);
    }

    public boolean isMediumBlob() {
      return this.equals(MEDIUMBLOB);
    }

    public boolean isLongText() {
      return this.equals(LONGTEXT);
    }

    public boolean isLongBlob() {
      return this.equals(LONGBLOB);
    }

    public boolean isText() {
      return this.equals(TEXT);
    }

    public boolean isBlob() {
      return this.equals(BLOB);
    }

    public boolean isChar() {
      return this.equals(CHAR);
    }

    public boolean isBinary() {
      return this.equals(BINARY);
    }

    public boolean isGeometry() {
      return this.equals(GEOMETRY);
    }
  }
}
