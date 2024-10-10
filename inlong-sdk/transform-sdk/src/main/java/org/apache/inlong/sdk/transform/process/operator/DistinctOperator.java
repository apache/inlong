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

import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
/**
 * DistinctOperator
 * description: value1 IS (NOT) DISTINCT FROM value2--Returns TRUE if two values are different.
 *              NULL values are treated as identical here.
 * for example: 1 IS DISTINCT FROM NULL returns TRUE;
 *              NULL IS DISTINCT FROM NULL returns FALSE.
 *              1 IS NOT DISTINCT FROM NULL returns FALSE;
 *              NULL IS NOT DISTINCT FROM NULL returns TRUE.
 */
@TransformOperator(values = IsDistinctExpression.class)
public class DistinctOperator implements ExpressionOperator {

    private final ValueParser leftParser;

    private final ValueParser rightParser;

    private final boolean isNot;

    public DistinctOperator(IsDistinctExpression expr) {
        this.leftParser = OperatorTools.buildParser(expr.getLeftExpression());
        this.rightParser = OperatorTools.buildParser(expr.getRightExpression());
        this.isNot = expr.isNot();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean check(SourceData sourceData, int rowIndex, Context context) {
        Object leftValue = this.leftParser.parse(sourceData, rowIndex, context);
        Object rightValue = this.rightParser.parse(sourceData, rowIndex, context);
        boolean res;

        if (leftValue == null && rightValue == null) {
            res = false;
        } else if (leftValue == null || rightValue == null) {
            res = true;
        } else {
            res = OperatorTools.compareValue((Comparable) leftValue, (Comparable) rightValue) != 0;
        }

        return isNot != res;
    }
}
