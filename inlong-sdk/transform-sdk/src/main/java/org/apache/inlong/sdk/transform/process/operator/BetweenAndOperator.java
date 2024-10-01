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

package org.apache.inlong.sdk.transform.process.operator;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.operators.relational.Between;
/**
 * BetweenAndOperator
 *
 * This class implements the ExpressionOperator interface to handle the SQL BETWEEN AND operator.
 * It checks if a left value is between start and end value.
 */
@TransformOperator(values = Between.class)
public class BetweenAndOperator implements ExpressionOperator {

    private final ValueParser left;
    private final ValueParser start;
    private final ValueParser end;
    private final boolean isNot;

    public BetweenAndOperator(Between expr) {
        this.left = OperatorTools.buildParser(expr.getLeftExpression());
        this.start = OperatorTools.buildParser(expr.getBetweenExpressionStart());
        this.end = OperatorTools.buildParser(expr.getBetweenExpressionEnd());
        this.isNot = expr.isNot();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean check(SourceData sourceData, int rowIndex, Context context) {
        Comparable leftValue = (Comparable) this.left.parse(sourceData, rowIndex, context);
        Comparable startValue = (Comparable) this.start.parse(sourceData, rowIndex, context);
        Comparable endValue = (Comparable) this.end.parse(sourceData, rowIndex, context);

        boolean result = OperatorTools.compareValue(leftValue, startValue) >= 0 &&
                OperatorTools.compareValue(leftValue, endValue) <= 0;

        return this.isNot != result;
    }
}