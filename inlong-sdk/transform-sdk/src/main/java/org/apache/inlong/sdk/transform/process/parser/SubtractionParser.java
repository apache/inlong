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
import org.apache.inlong.sdk.transform.process.utils.DateUtil;

import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.time.temporal.ChronoField;
import java.util.Map;

/**
 * SubtractionParser
 */
@TransformParser(values = Subtraction.class)
public class SubtractionParser implements ValueParser {

    private ValueParser left;

    private ValueParser right;

    public SubtractionParser(Subtraction expr) {
        this.left = OperatorTools.buildParser(expr.getLeftExpression());
        this.right = OperatorTools.buildParser(expr.getRightExpression());
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (this.left instanceof IntervalParser && this.right instanceof IntervalParser) {
            return null;
        } else if (this.left instanceof IntervalParser || this.right instanceof IntervalParser) {
            IntervalParser intervalParser = null;
            ValueParser dateParser = null;
            if (this.left instanceof IntervalParser) {
                intervalParser = (IntervalParser) this.left;
                dateParser = this.right;
            } else {
                intervalParser = (IntervalParser) this.right;
                dateParser = this.left;
            }
            Object intervalPairObj = intervalParser.parse(sourceData, rowIndex, context);
            Object dateObj = dateParser.parse(sourceData, rowIndex, context);
            if (intervalPairObj == null || dateObj == null) {
                return null;
            }
            return DateUtil.dateAdd(OperatorTools.parseString(dateObj),
                    (Pair<Integer, Map<ChronoField, Long>>) intervalPairObj, -1);
        } else {
            return numericalOperation(sourceData, rowIndex, context);
        }
    }

    private BigDecimal numericalOperation(SourceData sourceData, int rowIndex, Context context) {
        Object leftObj = this.left.parse(sourceData, rowIndex, context);
        Object rightObj = this.right.parse(sourceData, rowIndex, context);
        if (leftObj == null || rightObj == null) {
            return null;
        }
        BigDecimal leftValue = OperatorTools.parseBigDecimal(leftObj);
        BigDecimal rightValue = OperatorTools.parseBigDecimal(rightObj);
        return leftValue.subtract(rightValue);
    }
}
