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

package org.apache.inlong.sdk.transform.process.function;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;
import org.apache.inlong.sdk.transform.process.utils.DateUtil;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.List;

/**
 * TimeDiffFunction   ->   TIMEDIFF(expr1,expr2)
 * description:
 * - return NULL if expr1 or expr2 is NULL and the conversion types of expr1 and expr2 are different
 * - returns expr1 − expr2 expressed as a time value.
 * Note: expr1 and expr2 are strings converted to TIME or DATETIME expressions.
 */
@TransformFunction(names = {"timediff", "time_diff"})
@Slf4j
public class TimeDiffFunction implements ValueParser {

    private final ValueParser leftDateParser;
    private final ValueParser rightDateParser;

    public TimeDiffFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        leftDateParser = OperatorTools.buildParser(expressions.get(0));
        rightDateParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object leftDateObj = leftDateParser.parse(sourceData, rowIndex, context);
        Object rightDateObj = rightDateParser.parse(sourceData, rowIndex, context);
        if (leftDateObj == null || rightDateObj == null) {
            return null;
        }
        String leftDate = OperatorTools.parseString(leftDateObj);
        String rightDate = OperatorTools.parseString(rightDateObj);
        if (leftDate.isEmpty() || rightDate.isEmpty()) {
            return null;
        }
        boolean leftHasTime = leftDate.indexOf(' ') != -1;
        boolean rightHasTime = rightDate.indexOf(' ') != -1;

        boolean leftHasMicros = leftDate.indexOf('.') != -1;
        boolean rightHasMicros = rightDate.indexOf('.') != -1;

        try {
            Temporal left = null, right = null;
            if (leftHasTime && rightHasTime) {
                left = DateUtil.parseLocalDateTime(leftDate);
                right = DateUtil.parseLocalDateTime(rightDate);
            } else if (!leftHasTime && !rightHasTime) {
                left = DateUtil.parseLocalTime(leftDate);
                right = DateUtil.parseLocalTime(rightDate);
            }
            if (left == null || right == null) {
                return null;
            }
            long nanoDifference = ChronoUnit.NANOS.between(right, left);

            // Convert nanoseconds to total seconds and remaining microseconds
            long totalSeconds = nanoDifference / 1_000_000_000;
            long microseconds = Math.abs((nanoDifference % 1_000_000_000) / 1_000);

            // Handle negative duration
            boolean isNegative = nanoDifference < 0;

            // Convert totalSeconds to hours, minutes, and seconds
            long absTotalSeconds = Math.abs(totalSeconds);
            long hours = absTotalSeconds / 3600;
            long minutes = (absTotalSeconds % 3600) / 60;
            long seconds = absTotalSeconds % 60;

            String between = String.format("%s%02d:%02d:%02d", isNegative ? "-" : "",
                    Math.abs(hours), Math.abs(minutes), Math.abs(seconds));

            if (leftHasMicros || rightHasMicros) {
                between += String.format(".%06d", Math.abs(microseconds));
            }
            return between;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }
}
