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

import org.apache.commons.lang3.tuple.Pair;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.LinkedHashMap;
import java.util.Map;

public class DateUtil {

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
     * @param intervalPair Interval parsing results
     * @param sign         If the sign is positive or negative, it indicates addition or subtraction
     * @return Calculation result string
     */
    public static String dateAdd(String dateStr, Pair<Integer, Map<ChronoField, Long>> intervalPair, int sign) {

        if (sign < 0) {
            sign = -1;
        } else if (sign > 0) {
            sign = 1;
        } else {
            return null;
        }

        Object dateParserObj = parseLocalDateTime(dateStr);
        if (dateParserObj != null) {
            return addDateTime(intervalPair, sign, (LocalDateTime) dateParserObj, dateStr);
        }
        dateParserObj = parseLocalTime(dateStr);
        if (dateParserObj != null) {
            return addTime(intervalPair, sign, (LocalTime) dateParserObj, dateStr);
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

    public static DateTimeFormatter getDateTimeFormatter(String formatStr) {
        DateTimeFormatter formatter = DATE_TIME_FORMATTER_MAP.get(formatStr);
        if (formatter != null) {
            return formatter;
        }
        return TIME_FORMATTER_MAP.get(formatStr);
    }

    private static String addDateTime(Pair<Integer, Map<ChronoField, Long>> intervalPair, int sign,
            LocalDateTime dateTime, String dataStr) {
        int factor = intervalPair.getKey();
        Map<ChronoField, Long> valueMap = intervalPair.getValue();

        boolean hasTime = dataStr.indexOf(' ') != -1;
        boolean hasMicroSecond = dataStr.indexOf('.') != -1;

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

    private static String addTime(Pair<Integer, Map<ChronoField, Long>> intervalPair, int sign, LocalTime time,
            String dataStr) {
        int factor = intervalPair.getKey();
        Map<ChronoField, Long> valueMap = intervalPair.getValue();

        boolean hasMicroSecond = dataStr.indexOf('.') != -1;

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
