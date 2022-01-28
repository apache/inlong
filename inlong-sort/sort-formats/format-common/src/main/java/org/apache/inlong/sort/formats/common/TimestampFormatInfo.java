/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.formats.common;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The format information for {@link Timestamp}s.
 */
public class TimestampFormatInfo implements BasicFormatInfo<Timestamp> {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_FORMAT = "format";

    private static final int MIN_PRECISION = 0;
    private static final int MAX_PRECISION = 9;
    private static final int DEFAULT_PRECISION = 6;

    @JsonProperty(FIELD_FORMAT)
    @Nonnull
    private final String format;

    @JsonIgnore
    @Nullable
    private final SimpleDateFormat simpleDateFormat;

    @JsonProperty("precision")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Integer precision;

    public TimestampFormatInfo(String format) {
        this(format, DEFAULT_PRECISION);
    }

    @JsonCreator
    public TimestampFormatInfo(
            @JsonProperty(FIELD_FORMAT) @Nonnull String format,
            @JsonProperty("precision") Integer precision
    ) {
        this.format = format;

        if (!format.equals("MICROS")
                && !format.equals("MILLIS")
                && !format.equals("SECONDS")) {
            this.simpleDateFormat = new SimpleDateFormat(format);
        } else {
            this.simpleDateFormat = null;
        }

        if (precision == null) {
            this.precision = DEFAULT_PRECISION;
        } else if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(
                    String.format("Timestamp precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        } else {
            this.precision = precision;
        }
    }

    public TimestampFormatInfo() {
        this("yyyy-MM-dd hh:mm:ss");
    }

    @Nonnull
    public String getFormat() {
        return format;
    }

    public Integer getPrecision() {
        return precision;
    }

    @Override
    public TimestampTypeInfo getTypeInfo() {
        return TimestampTypeInfo.INSTANCE;
    }

    @Override
    public String serialize(Timestamp timestamp) {

        switch (format) {
            case "MICROS": {
                long millis = timestamp.getTime();
                long micros = TimeUnit.MILLISECONDS.toMicros(millis);
                return Long.toString(micros);
            }
            case "MILLIS": {
                long millis = timestamp.getTime();
                return Long.toString(millis);
            }
            case "SECONDS": {
                long millis = timestamp.getTime();
                long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
                return Long.toString(seconds);
            }
            default: {
                if (simpleDateFormat == null) {
                    throw new IllegalStateException();
                }

                return simpleDateFormat.format(timestamp);
            }
        }
    }

    @Override
    public Timestamp deserialize(String text) throws ParseException {
        switch (format) {
            case "MICROS": {
                long micros = Long.parseLong(text);
                long millis = TimeUnit.MICROSECONDS.toMillis(micros);
                return new Timestamp(millis);
            }
            case "MILLIS": {
                long millis = Long.parseLong(text);
                return new Timestamp(millis);
            }
            case "SECONDS": {
                long seconds = Long.parseLong(text);
                long millis = TimeUnit.SECONDS.toMillis(seconds);
                return new Timestamp(millis);
            }
            default: {
                if (simpleDateFormat == null) {
                    throw new IllegalStateException();
                }

                Date date = simpleDateFormat.parse(text);
                return new Timestamp(date.getTime());
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimestampFormatInfo that = (TimestampFormatInfo) o;
        return format.equals(that.format) && Objects.equals(precision, that.precision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, precision);
    }

    @Override
    public String toString() {
        return "TimestampFormatInfo{format='" + format + '\'' + ", precision=" + precision + '}';
    }
}
