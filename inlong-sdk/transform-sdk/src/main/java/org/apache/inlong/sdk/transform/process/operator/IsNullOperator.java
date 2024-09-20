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

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;

/**
 * IsNullOperator ->  expr is [not] null
 * Description:  expr can accept ValueParser and ExpressionOperation:
 * return the judgment result
 */
@Slf4j
@TransformOperator(values = IsNullExpression.class)
public class IsNullOperator implements ExpressionOperator {

    private ValueParser leftParser;
    private ExpressionOperator operator;
    private final boolean isNot;

    public IsNullOperator(IsNullExpression expr) {
        try {
            leftParser = OperatorTools.buildParser(expr.getLeftExpression());
        } catch (Exception e) {
            operator = OperatorTools.buildOperator(expr.getLeftExpression());
        }
        isNot = expr.isNot();
    }

    @Override
    public boolean check(SourceData sourceData, int rowIndex, Context context) {
        Object leftObj = null;
        if (leftParser != null) {
            leftObj = leftParser.parse(sourceData, rowIndex, context);
        } else {
            leftObj = operator.check(sourceData, rowIndex, context);
        }
        if (isNot) {
            return !(leftObj == null);
        } else {
            return leftObj == null;
        }
    }
}
