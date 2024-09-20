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

package org.apache.inlong.sdk.transform.process.parser;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.IntervalExpression;
import org.apache.commons.lang3.tuple.Pair;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IntervalParser  <-> INTERVAL expr unit ->  Pair(factor,Map(ChronoField,Count)):
 * <p>
 * `factor`:
 * <p>
 *    1) `expr` can accept strings starting with '-', representing the meaning of subtraction.
 *       So the positive or negative sign of `factor` indicates whether `expr` starts with a '-' or not.
 * <p>
 *    2) For units like WEEK and QUARTER, it is not easy to parse,
 *       so WEEK -> ( unit=DAY, adb(factor)=7 ); QUARTER -> ( unit=MONTH, adb(factor)=3 ).
 * <p>
 * `Map(ChronoField,Count)`:
 * <p>
 *    IntervalParser will automatically match the corresponding DateTimeFormatter based on the input `expr`,
 *    Based on DateTimeFormatter, IntervalParser will parse the incoming units and store them in a Map.
 * <p>
 *
 *  In addition,acceptable expression parsing and specifying parameters in two ways:
 *      1) interval rowName year -> expression
 *      3) interval 2 year       -> fixed parameter
 */
@Slf4j
@TransformParser(values = IntervalExpression.class)
public class IntervalParser implements ValueParser {

    private final String intervalType;
    private final ValueParser dateParser;
    private final String parameter;

    private static final List<ChronoField> CHRONO_FIELD_LIST = Arrays.asList(ChronoField.YEAR,
            ChronoField.MONTH_OF_YEAR,
            ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE,
            ChronoField.MICRO_OF_SECOND);
    private static final Map<String, DateTimeFormatter> DT_FORMATTER_MAP = new ConcurrentHashMap<>();

    static {
        DT_FORMATTER_MAP.put("SECOND_MICROSECOND", DateTimeFormatter.ofPattern("s.SSSSSS"));
        DT_FORMATTER_MAP.put("MINUTE_MICROSECOND", DateTimeFormatter.ofPattern("m:s.SSSSSS"));
        DT_FORMATTER_MAP.put("MINUTE_SECOND", DateTimeFormatter.ofPattern("m:s"));
        DT_FORMATTER_MAP.put("HOUR_MICROSECOND", DateTimeFormatter.ofPattern("H:m:s.SSSSSS"));
        DT_FORMATTER_MAP.put("HOUR_SECOND", DateTimeFormatter.ofPattern("H:m:s"));
        DT_FORMATTER_MAP.put("HOUR_MINUTE", DateTimeFormatter.ofPattern("H:m"));
        DT_FORMATTER_MAP.put("DAY_MICROSECOND", DateTimeFormatter.ofPattern("d H:m:s.SSSSSS"));
        DT_FORMATTER_MAP.put("DAY_SECOND", DateTimeFormatter.ofPattern("d H:m:s"));
        DT_FORMATTER_MAP.put("DAY_MINUTE", DateTimeFormatter.ofPattern("d H:m"));
        DT_FORMATTER_MAP.put("DAY_HOUR", DateTimeFormatter.ofPattern("d H"));
        DT_FORMATTER_MAP.put("YEAR_MONTH", DateTimeFormatter.ofPattern("y-M"));

        DT_FORMATTER_MAP.put("MICROSECOND", DateTimeFormatter.ofPattern("SSSSSS"));
        DT_FORMATTER_MAP.put("SECOND", DateTimeFormatter.ofPattern("s"));
        DT_FORMATTER_MAP.put("MINUTE", DateTimeFormatter.ofPattern("m"));
        DT_FORMATTER_MAP.put("HOUR", DateTimeFormatter.ofPattern("H"));
        DT_FORMATTER_MAP.put("DAY", DateTimeFormatter.ofPattern("d"));
        DT_FORMATTER_MAP.put("MONTH", DateTimeFormatter.ofPattern("M"));
        DT_FORMATTER_MAP.put("YEAR", DateTimeFormatter.ofPattern("y"));
    }

    public IntervalParser(IntervalExpression expr) {
        intervalType = expr.getIntervalType().toUpperCase();
        dateParser = OperatorTools.buildParser(expr.getExpression());
        if (dateParser == null) {
            parameter = expr.getParameter();
        } else {
            parameter = null;
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        DateTimeFormatter dateTimeFormatter = DT_FORMATTER_MAP.get(intervalType);

        String dataStr = parameter;
        if (dateParser != null) {
            Object dateObj = dateParser.parse(sourceData, rowIndex, context);
            if (dateObj == null) {
                return null;
            }
            dataStr = OperatorTools.parseString(dateObj);
        }

        int factor = 1;
        if (dateTimeFormatter == null) {
            if ("WEEK".equals(intervalType)) {
                dateTimeFormatter = DT_FORMATTER_MAP.get("DAY");
                factor = 7;
            } else if ("QUARTER".equals(intervalType)) {
                dateTimeFormatter = DT_FORMATTER_MAP.get("MONTH");
                factor = 3;
            } else {
                return null;
            }
        }

        try {
            factor = dataStr.charAt(0) == '-' ? -factor : factor;
            if (factor < 0) {
                dataStr = dataStr.substring(1);
            }
            TemporalAccessor temporalAccessor = dateTimeFormatter.parse(dataStr);
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
            return Pair.of(factor, map);
        } catch (Exception e) {
            log.error("Interval parse error", e);
            return null;
        }
    }
}
