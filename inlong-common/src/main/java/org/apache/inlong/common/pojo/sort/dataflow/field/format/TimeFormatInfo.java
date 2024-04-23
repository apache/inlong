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

package org.apache.inlong.common.pojo.sort.dataflow.field.format;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;

import java.sql.Time;
import java.text.ParseException;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.common.pojo.sort.dataflow.field.format.Constants.DATE_AND_TIME_STANDARD_ISO_8601;
import static org.apache.inlong.common.pojo.sort.dataflow.field.format.Constants.DATE_AND_TIME_STANDARD_SQL;

/**
 * The format information for {@link Time}s.
 */
public class TimeFormatInfo implements BasicFormatInfo<Time> {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_FORMAT = "format";
    // to support avro format, precision must be less than 3
    private static final int DEFAULT_PRECISION_FOR_TIMESTAMP = 2;
    @JsonProperty(FIELD_FORMAT)
    @Nonnull
    private final String format;
    @JsonProperty("precision")
    private int precision;

    @JsonCreator
    public TimeFormatInfo(
            @JsonProperty(FIELD_FORMAT) @Nonnull String format,
            @JsonProperty("precision") int precision) {
        this.format = format;
        this.precision = precision;
        if (!format.equals("SECONDS")
                && !format.equals("MILLIS")
                && !format.equals("MICROS")
                && !DATE_AND_TIME_STANDARD_SQL.equals(format)
                && !DATE_AND_TIME_STANDARD_ISO_8601.equals(format)) {
            FastDateFormat.getInstance(format);
        }
    }

    public TimeFormatInfo(@JsonProperty(FIELD_FORMAT) @Nonnull String format) {
        this(format, DEFAULT_PRECISION_FOR_TIMESTAMP);
    }

    public TimeFormatInfo() {
        this("HH:mm:ss", DEFAULT_PRECISION_FOR_TIMESTAMP);
    }

    @Nonnull
    public String getFormat() {
        return format;
    }

    @Override
    public TimeTypeInfo getTypeInfo() {
        return TimeTypeInfo.INSTANCE;
    }

    @Override
    public String serialize(Time time) {
        switch (format) {
            case "MICROS": {
                long millis = time.getTime();
                long micros = TimeUnit.MILLISECONDS.toMicros(millis);
                return Long.toString(micros);
            }
            case "MILLIS": {
                long millis = time.getTime();
                return Long.toString(millis);
            }
            case "SECONDS": {
                long millis = time.getTime();
                long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
                return Long.toString(seconds);
            }
            default: {
                return FastDateFormat.getInstance(format).format(time.getTime());
            }
        }
    }

    @Override
    public Time deserialize(String text) throws ParseException {
        switch (format) {
            case "MICROS": {
                long micros = Long.parseLong(text);
                long millis = TimeUnit.MICROSECONDS.toMillis(micros);
                return new Time(millis);
            }
            case "MILLIS": {
                long millis = Long.parseLong(text);
                return new Time(millis);
            }
            case "SECONDS": {
                long seconds = Long.parseLong(text);
                long millis = TimeUnit.SECONDS.toMillis(seconds);
                return new Time(millis);
            }
            default: {
                Date date = FastDateFormat.getInstance(format).parse(text);
                return new Time(date.getTime());
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

        TimeFormatInfo that = (TimeFormatInfo) o;
        return format.equals(that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format);
    }

    @Override
    public String toString() {
        return "TimeFormatInfo{" + "format='" + format + '\'' + '}';
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }
}
