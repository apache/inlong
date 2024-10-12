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

package org.apache.inlong.sdk.transform.process.function.string;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.nio.charset.Charset;
import java.util.List;

/**
 * BitLengthFunction  -> bit_length(string,[charset])
 * description:
 * - Return NULL if the string is NULL.
 * - Return number of bits in string.
 */
@Slf4j
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "bit_length"}, parameter = "(String str,[String charset])", descriptions = {
                "- Return \"\" if the 'str' is NULL;", "- Return number of bits in 'str'.",
                "Note: Charset is aligned with the JVM by default."}, examples = {"bit_length(\"hello world\") = 88",
                        "bit_length(\"hello 你好\",\"utf-8\") = 96"})
public class BitLengthFunction implements ValueParser {

    private final ValueParser stringParser;
    private final ValueParser charsetParser;
    private final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    public BitLengthFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() == 2) {
            charsetParser = OperatorTools.buildParser(expressions.get(1));
        } else {
            charsetParser = null;
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObject = stringParser.parse(sourceData, rowIndex, context);
        if (stringObject == null) {
            return null;
        }
        try {
            Charset charset = DEFAULT_CHARSET;
            if (charsetParser != null) {
                Object charsetObj = charsetParser.parse(sourceData, rowIndex, context);
                if (charsetObj != null) {
                    charset = Charset.forName(OperatorTools.parseString(charsetObj));
                }
            }
            return OperatorTools.parseString(stringObject).getBytes(charset).length * 8;
        } catch (Exception e) {
            log.error("Analysis failed", e);
            return null;
        }
    }
}
