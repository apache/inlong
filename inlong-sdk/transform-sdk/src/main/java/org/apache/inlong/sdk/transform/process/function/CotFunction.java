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

import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;

/**
 * CotFunction
 * description: cot(numeric) -- returns the cotangent of the numeric (in radians)
 */
@TransformFunction(names = {"cot"})
public class CotFunction implements ValueParser {

    private final ValueParser valueParser;

    public CotFunction(Function expr) {
        // 使用OperatorTools构建解析器来处理表达式中的参数
        this.valueParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        // 解析输入的参数
        Object valueObj = valueParser.parse(sourceData, rowIndex, context);

        // 将解析结果转换为BigDecimal以处理数值计算
        BigDecimal value = OperatorTools.parseBigDecimal(valueObj);

        // 计算tan(x)并取倒数以求得cot(x)
        double tanValue = Math.tan(value.doubleValue());
        if (tanValue == 0) {
            throw new ArithmeticException("Cotangent undefined for this input, tan(x) is zero.");
        }
        return 1.0 / tanValue;
    }
}
