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
import org.apache.inlong.sdk.transform.process.operator.ExpressionOperator;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;

import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.WhenClause;

import java.util.ArrayList;
import java.util.List;
/**
 * CASE value WHEN compare_value THEN result [WHEN compare_value THEN result ...] [ELSE result] END
 * CASE WHEN condition THEN result [WHEN condition THEN result ...] [ELSE result] END
 *
 * The first CASE syntax returns the result for the first value=compare_value comparison that is true.
 * The second syntax returns the result for the first condition that is true.
 * If no comparison or condition is true, the result after ELSE is returned, or NULL if there is no ELSE part.
 */
@TransformParser(values = CaseExpression.class)
public class CaseParser implements ValueParser {

    private final ValueParser switchValue;
    private final List<CaseWhen> caseWhens;
    private final ValueParser elseResult;

    public CaseParser(CaseExpression expr) {
        this.switchValue = OperatorTools.buildParser(expr.getSwitchExpression());
        this.caseWhens = new ArrayList<>();
        this.elseResult = OperatorTools.buildParser(expr.getElseExpression());
        for (WhenClause whenClause : expr.getWhenClauses()) {
            Object condition = null;
            if (switchValue == null) {
                condition = OperatorTools.buildOperator(whenClause.getWhenExpression());
            } else {
                condition = OperatorTools.buildParser(whenClause.getWhenExpression());
            }
            ValueParser result = OperatorTools.buildParser(whenClause.getThenExpression());
            this.caseWhens.add(new CaseWhen(condition, result));
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (switchValue == null) {
            for (CaseWhen caseWhen : caseWhens) {
                if (((ExpressionOperator) caseWhen.condition).check(sourceData, rowIndex, context)) {
                    return caseWhen.result.parse(sourceData, rowIndex, context);
                }
            }
        } else {
            for (CaseWhen caseWhen : caseWhens) {
                Comparable left = (Comparable) switchValue.parse(sourceData, rowIndex, context);
                Comparable right = (Comparable) ((ValueParser) caseWhen.condition).parse(sourceData, rowIndex, context);
                if (OperatorTools.compareValue(left, right) == 0) {
                    return caseWhen.result.parse(sourceData, rowIndex, context);
                }
            }
        }
        return elseResult == null ? null : elseResult.parse(sourceData, rowIndex, context);
    }

    private static class CaseWhen {

        private final Object condition;
        private final ValueParser result;

        public CaseWhen(Object condition, ValueParser result) {
            this.condition = condition;
            this.result = result;
        }
    }
}