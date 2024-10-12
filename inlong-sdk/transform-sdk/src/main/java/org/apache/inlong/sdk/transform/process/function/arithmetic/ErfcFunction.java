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

package org.apache.inlong.sdk.transform.process.function.arithmetic;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;
import org.apache.commons.math3.special.Erf;

/**
 * ErfcFunction  ->  erfc(numeric)
 * Description:
 * - Return NULL if 'numeric' is NULL;
 * - Return complementary error (1 - erf('numeric'), without loss of precision for large inputs).
 */
@TransformFunction(names = {"erfc"}, parameter = "(Numeric numeric)", descriptions = {
        "- Return \"\" if 'numeric' is NULL;",
        "- Return complementary error (1 - erf('numeric'), without loss of precision for large inputs)."
}, examples = {
        "erfc(1) = 0.15729920705028488"
})
public class ErfcFunction implements ValueParser {

    private final ValueParser numParser;

    public ErfcFunction(Function expr) {
        numParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numObj = numParser.parse(sourceData, rowIndex, context);
        if (numObj == null) {
            return null;
        }
        double num = OperatorTools.parseBigDecimal(numObj).doubleValue();
        return Erf.erfc(num);
    }
}
