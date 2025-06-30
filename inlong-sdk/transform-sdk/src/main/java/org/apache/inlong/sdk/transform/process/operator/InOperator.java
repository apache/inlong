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

import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;

import java.util.ArrayList;
import java.util.List;

/**
 * InExpression
 * 
 */
@TransformOperator(values = InExpression.class)
public class InOperator implements ExpressionOperator {

    private final ValueParser left;
    private final List<ValueParser> right;

    public InOperator(InExpression expr) {
        this.left = OperatorTools.buildParser(expr.getLeftExpression());
        ItemsList itemsList = expr.getRightItemsList();
        this.right = new ArrayList<>();
        if (itemsList instanceof ExpressionList) {
            ((ExpressionList) itemsList).getExpressions().forEach(v -> this.right.add(OperatorTools.buildParser(v)));
        } else if (itemsList instanceof MultiExpressionList) {
            List<ExpressionList> exprListList = ((MultiExpressionList) itemsList).getExpressionLists();
            for (ExpressionList exprList : exprListList) {
                exprList.getExpressions().forEach(v -> this.right.add(OperatorTools.buildParser(v)));
            }
        } else if (itemsList instanceof NamedExpressionList) {
            ((NamedExpressionList) itemsList).getExpressions()
                    .forEach(v -> this.right.add(OperatorTools.buildParser(v)));
        }
    }

    /**
     * check
     * @param sourceData
     * @param rowIndex
     * @return
     */
    @SuppressWarnings("rawtypes")
    @Override
    public boolean check(SourceData sourceData, int rowIndex, Context context) {
        Comparable leftValue = (Comparable) this.left.parse(sourceData, rowIndex, context);
        for (ValueParser parser : right) {
            Comparable rightValue = (Comparable) parser.parse(sourceData, rowIndex, context);
            if (OperatorTools.compareValue(leftValue, rightValue) == 0) {
                return true;
            }
        }
        return false;
    }
}
