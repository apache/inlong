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

import net.sf.jsqlparser.expression.ArrayExpression;

import java.util.List;

/**
 * ArrayParser
 * Description: Support to get the value from array
 */
@TransformParser(values = ArrayExpression.class)
public class ArrayParser implements ValueParser {

    private final ValueParser left;

    private final ValueParser right;

    public ArrayParser(ArrayExpression expr) {
        this.left = OperatorTools.buildParser(expr.getObjExpression());
        this.right = OperatorTools.buildParser(expr.getIndexExpression());
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object leftValue = this.left.parse(sourceData, rowIndex, context);
        Object rightValue = this.right.parse(sourceData, rowIndex, context);

        if (leftValue instanceof List<?> && rightValue instanceof Number) {
            List<?> leftObj = (List<?>) leftValue;
            Number rightObj = (Number) rightValue;
            return leftObj.get(rightObj.intValue());
        }
        return null;
    }
}
