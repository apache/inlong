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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * UrlDecodeFunction  ->  url_decode(str[, charset])
 * description:
 * - Return NULL if 'str' is NULL, or there is an issue with the decoding process(such as encountering an illegal
 *          escape pattern), or the encoding scheme is not supported;
 * - Return the result of decoding a given 'str' in 'application/x-www-form-urlencoded' format using the charset(default:UTF-8) encoding scheme.
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "url_decode"}, parameter = "(String str[, String charset])", descriptions = {
                "- Return \"\" if 'str' is NULL, or there is an issue with the decoding process(such as encountering an "
                        +
                        "illegal escape pattern), or the encoding scheme is not supported;",
                "- Return the result of decoding a given 'str' in 'application/x-www-form-urlencoded' format using the "
                        +
                        "charset(default:UTF-8) encoding scheme."
        }, examples = {
                "url_decode('https%3A%2F%2Fapache.inlong.com%2Fsearch%3Fq%3Djava+url+encode') = \"https://apache.inlong.com/search?q=java url encode\"",
                "url_decode('https%3A%2F%2Fapache.inlong.com%2Fsearch%3Fq%3Djava+url+encode','UTF-8') = \"https://apache.inlong.com/search?q=java url encode\""})
public class UrlDecodeFunction implements ValueParser {

    private final ValueParser stringParser;
    private final ValueParser charsetParser;
    private final String exprKey;

    public UrlDecodeFunction(Function expr) {
        List<Expression> params = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(params.get(0));
        charsetParser = params.size() > 1 ? OperatorTools.buildParser(params.get(1)) : null;
        exprKey = expr.toString();
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Map<String, Object> runtimeParams = context.getRuntimeParams();
        if (runtimeParams.containsKey(exprKey)) {
            return runtimeParams.get(exprKey);
        }
        Object stringObj = stringParser.parse(sourceData, rowIndex, context);
        if (stringObj == null) {
            runtimeParams.put(exprKey, null);
            return null;
        }
        String string = OperatorTools.parseString(stringObj);
        if (string == null) {
            runtimeParams.put(exprKey, null);
            return null;
        }

        try {
            if (charsetParser == null) {
                String exprValue = URLDecoder.decode(string, StandardCharsets.UTF_8.toString());
                runtimeParams.put(exprKey, exprValue);
                return exprValue;
            } else {
                Object charsetObj = charsetParser.parse(sourceData, rowIndex, context);
                if (charsetObj == null) {
                    runtimeParams.put(exprKey, null);
                    return null;
                }
                String charset = OperatorTools.parseString(charsetObj);
                if (charset == null) {
                    runtimeParams.put(exprKey, null);
                    return null;
                }
                String exprValue = URLDecoder.decode(string, charset);
                runtimeParams.put(exprKey, exprValue);
                return exprValue;
            }
        } catch (Exception e) {
            runtimeParams.put(exprKey, null);
            return null;
        }
    }
}
