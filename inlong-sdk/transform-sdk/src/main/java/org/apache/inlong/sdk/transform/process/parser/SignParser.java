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

import net.sf.jsqlparser.expression.SignedExpression;

import java.math.BigDecimal;

/**
 * SignParser
 * 
 */
@TransformParser(values = SignedExpression.class)
public class SignParser implements ValueParser {

    private final Integer sign;
    private final ValueParser number;

    public SignParser(SignedExpression expr) {
        sign = expr.getSign() == '-' ? -1 : 1;
        number = OperatorTools.buildParser(expr.getExpression());
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numberObject = number.parse(sourceData, rowIndex, context);
        BigDecimal numberValue = OperatorTools.parseBigDecimal(numberObject);
        return numberValue.multiply(new BigDecimal(sign));
    }
}
