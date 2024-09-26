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

package org.apache.inlong.sdk.transform.process.utils;

import org.apache.inlong.sdk.transform.process.pojo.IntervalInfo;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DateUtil {

    private static final List<ChronoField> CHRONO_FIELD_LIST = Arrays.asList(ChronoField.YEAR,
            ChronoField.MONTH_OF_YEAR,
            ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE,
            ChronoField.MICRO_OF_SECOND);

    // Need to follow this order
    private static final Map<String, DateTimeFormatter> DATE_TIME_FORMATTER_MAP = new LinkedHashMap<>();
    private static final Map<String, DateTimeFormatter> TIME_FORMATTER_MAP = new LinkedHashMap<>();
    public static String YEAR_TO_MICRO = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static String YEAR_TO_SECOND = "yyyy-MM-dd HH:mm:ss";
    public static String YEAR_TO_MONTH = "yyyy-MM-dd";
    public static String HOUR_TO_MICRO = "HH:mm:ss.SSSSSS";
    public static String HOUR_TO_SECOND = "HH:mm:ss";

    static {
        DATE_TIME_FORMATTER_MAP.put(YEAR_TO_MICRO, DateTimeFormatter.ofPattern(YEAR_TO_MICRO));
        DATE_TIME_FORMATTER_MAP.put(YEAR_TO_SECOND, DateTimeFormatter.ofPattern(YEAR_TO_SECOND));
        DATE_TIME_FORMATTER_MAP.put(YEAR_TO_MONTH, DateTimeFormatter.ofPattern(YEAR_TO_MONTH));

        TIME_FORMATTER_MAP.put(HOUR_TO_MICRO, DateTimeFormatter.ofPattern(HOUR_TO_MICRO));
        TIME_FORMATTER_MAP.put(HOUR_TO_SECOND, DateTimeFormatter.ofPattern(HOUR_TO_SECOND));
    }

    /**
     * Time calculation
     *
     * @param dateStr      Time parameter string
     * @param intervalInfo Interval parsing results
     * @param isPositive   True is positive, false is negative
     * @return Calculation result string
     */
    public static String dateTypeAdd(String dateStr, IntervalInfo intervalInfo, boolean isPositive) {
        Object dateParserObj = parseLocalDateTime(dateStr);
        if (dateParserObj != null) {
            return addDateTime(dateStr, (LocalDateTime) dateParserObj, intervalInfo, isPositive);
        }
        dateParserObj = parseLocalTime(dateStr);
        if (dateParserObj != null) {
            return addTime(dateStr, (LocalTime) dateParserObj, intervalInfo, isPositive);
        }
        return null;
    }

    public static LocalDateTime dateAdd(LocalDateTime localDateTime, LocalTime localTime) {
        return localDateTime.plusHours(localTime.getHour())
                .plusMinutes(localTime.getMinute())
                .plusSeconds(localTime.getSecond())
                .plusNanos(localTime.getNano());
    }

    public static LocalDateTime parseLocalDateTime(String dateStr) {
        LocalDateTime dateParserObj = null;
        for (DateTimeFormatter dateTimeFormatter : DATE_TIME_FORMATTER_MAP.values()) {
            try {
                dateParserObj = LocalDateTime.parse(dateStr, dateTimeFormatter);
            } catch (Exception e) {
                try {
                    dateParserObj = LocalDate.parse(dateStr, dateTimeFormatter).atStartOfDay();
                } catch (Exception ignored) {

                }
            }
            if (dateParserObj != null) {
                return dateParserObj;
            }
        }
        return null;
    }

    public static LocalTime parseLocalTime(String dateStr) {
        LocalTime dateParserObj = null;
        for (DateTimeFormatter dateTimeFormatter : TIME_FORMATTER_MAP.values()) {
            try {
                dateParserObj = LocalTime.parse(dateStr, dateTimeFormatter);
            } catch (Exception ignored) {

            }
            if (dateParserObj != null) {
                return dateParserObj;
            }
        }
        return null;
    }

    public static IntervalInfo parseIntervalInfo(DateTimeFormatter dateTimeFormatter, String dateStr, int factor) {
        TemporalAccessor temporalAccessor = dateTimeFormatter.parse(dateStr);
        HashMap<ChronoField, Long> map = new HashMap<>();
        for (ChronoField field : CHRONO_FIELD_LIST) {
            try {
                long num = temporalAccessor.getLong(field);
                if (num == 0) {
                    continue;
                }
                map.put(field, temporalAccessor.getLong(field));
            } catch (Exception ignored) {

            }
        }
        return new IntervalInfo(factor, map);
    }

    public static DateTimeFormatter getDateTimeFormatter(String formatStr) {
        DateTimeFormatter formatter = DATE_TIME_FORMATTER_MAP.get(formatStr);
        if (formatter != null) {
            return formatter;
        }
        return TIME_FORMATTER_MAP.get(formatStr);
    }

    /**
     *
     * @param dateStr The first time string
     * @param dateTime It is obtained by parsing dateStr
     * @param intervalInfo addend
     * @param isPositive   True is positive, false is negative
     * @return
     */
    public static String addDateTime(String dateStr, LocalDateTime dateTime,
            IntervalInfo intervalInfo, boolean isPositive) {
        int factor = intervalInfo.getFactor();
        Map<ChronoField, Long> valueMap = intervalInfo.getChronoMap();

        int sign = isPositive ? 1 : -1;

        boolean hasTime = dateStr.indexOf(' ') != -1;
        boolean hasMicroSecond = dateStr.indexOf('.') != -1;

        for (ChronoField field : valueMap.keySet()) {
            long amount = valueMap.get(field) * factor * sign;
            switch (field) {
                case MICRO_OF_SECOND:
                    hasTime = true;
                    hasMicroSecond = true;
                    dateTime = dateTime.plusNanos(amount * 1000L);
                    break;
                case SECOND_OF_MINUTE:
                    hasTime = true;
                    dateTime = dateTime.plusSeconds(amount);
                    break;
                case MINUTE_OF_HOUR:
                    hasTime = true;
                    dateTime = dateTime.plusMinutes(amount);
                    break;
                case HOUR_OF_DAY:
                    hasTime = true;
                    dateTime = dateTime.plusHours(amount);
                    break;
                case DAY_OF_MONTH:
                    dateTime = dateTime.plusDays(amount);
                    break;
                case MONTH_OF_YEAR:
                    dateTime = dateTime.plusMonths(amount);
                    break;
                case YEAR:
                    dateTime = dateTime.plusYears(amount);
                    break;
                default:
                    return null;
            }
        }
        if (hasTime) {
            if (hasMicroSecond) {
                return dateTime.format(DATE_TIME_FORMATTER_MAP.get(YEAR_TO_MICRO));
            } else {
                return dateTime.format(DATE_TIME_FORMATTER_MAP.get(YEAR_TO_SECOND));
            }
        }
        return dateTime.toLocalDate().toString();
    }

    /**
     *
     * @param dateStr The first time string
     * @param time It is obtained by parsing dateStr
     * @param intervalInfo addend
     * @param isPositive   True is positive, false is negative
     * @return
     */
    public static String addTime(String dateStr, LocalTime time,
            IntervalInfo intervalInfo, boolean isPositive) {
        int factor = intervalInfo.getFactor();
        Map<ChronoField, Long> valueMap = intervalInfo.getChronoMap();
        boolean hasMicroSecond = dateStr.indexOf('.') != -1;

        int sign = isPositive ? 1 : -1;

        for (ChronoField field : valueMap.keySet()) {
            long amount = valueMap.get(field) * factor * sign;
            switch (field) {
                case MICRO_OF_SECOND:
                    hasMicroSecond = true;
                    time = time.plusNanos(amount * 1000L);
                    break;
                case SECOND_OF_MINUTE:
                    time = time.plusSeconds(amount);
                    break;
                case MINUTE_OF_HOUR:
                    time = time.plusMinutes(amount);
                    break;
                case HOUR_OF_DAY:
                    time = time.plusHours(amount);
                    break;
                default:
                    return null;
            }
        }

        if (hasMicroSecond) {
            return time.format(TIME_FORMATTER_MAP.get(HOUR_TO_MICRO));
        } else {
            return time.format(TIME_FORMATTER_MAP.get(HOUR_TO_SECOND));
        }
    }

}
