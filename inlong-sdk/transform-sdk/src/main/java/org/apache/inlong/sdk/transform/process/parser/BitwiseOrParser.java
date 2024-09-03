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
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;

import java.math.BigInteger;

/**
 * BitwiseOrParser
 * 
 */
@Slf4j
@TransformParser(values = BitwiseOr.class)
public class BitwiseOrParser implements ValueParser {

    private final ValueParser left;

    private final ValueParser right;

    public BitwiseOrParser(BitwiseOr expr) {
        this.left = OperatorTools.buildParser(expr.getLeftExpression());
        this.right = OperatorTools.buildParser(expr.getRightExpression());
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        try {
            Object leftObj = this.left.parse(sourceData, rowIndex, context);
            Object rightObj = this.right.parse(sourceData, rowIndex, context);
            if (leftObj == null || rightObj == null) {
                return null;
            }
            BigInteger leftValue = OperatorTools.parseBigDecimal(leftObj).toBigInteger();
            BigInteger rightValue = OperatorTools.parseBigDecimal(rightObj).toBigInteger();
            return Long.toUnsignedString(leftValue.or(rightValue).longValue());
        } catch (Exception e) {
            log.error("Value parsing failed", e);
            return null;
        }
    }
}
