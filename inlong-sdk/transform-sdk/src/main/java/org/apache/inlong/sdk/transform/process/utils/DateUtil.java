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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DateUtil {

    // Need to follow this order
    private static final List<DateTimeFormatter> DATE_TIME_FORMATTER_LIST = Arrays.asList(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    private static final List<DateTimeFormatter> TIME_FORMATTER_LIST = Arrays.asList(
            DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"), DateTimeFormatter.ofPattern("HH:mm:ss"));

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

        Object dateParserObj = null;
        for (DateTimeFormatter dateTimeFormatter : DATE_TIME_FORMATTER_LIST) {
            try {
                dateParserObj = LocalDateTime.parse(dateStr, dateTimeFormatter);
            } catch (Exception e) {
                try {
                    dateParserObj = LocalDate.parse(dateStr, dateTimeFormatter).atStartOfDay();
                } catch (Exception ignored) {

                }
            }
            if (dateParserObj != null) {
                return addDateTime(intervalPair, sign, (LocalDateTime) dateParserObj, dateStr);
            }
        }

        for (DateTimeFormatter dateTimeFormatter : TIME_FORMATTER_LIST) {
            try {
                dateParserObj = LocalTime.parse(dateStr, dateTimeFormatter);
            } catch (Exception ignored) {

            }
            if (dateParserObj != null) {
                return addTime(intervalPair, sign, (LocalTime) dateParserObj, dateStr);
            }
        }

        return null;
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

        String result = dateTime.toLocalDate().toString();
        if (hasTime) {
            if (hasMicroSecond) {
                result += " " + dateTime.toLocalTime().format(TIME_FORMATTER_LIST.get(0));
            } else {
                result += " " + dateTime.toLocalTime().format(TIME_FORMATTER_LIST.get(1));
            }
        }
        return result;
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
            return time.format(TIME_FORMATTER_LIST.get(0));
        } else {
            return time.format(TIME_FORMATTER_LIST.get(1));
        }
    }

}
