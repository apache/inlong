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
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * UrlEncodeFunction  ->  url_encode(str)
 * description:
 * - Return NULL if 'str' is NULL, or there is an issue with the decoding process(such as encountering an illegal
 *          escape pattern), or the encoding scheme is not supported;
 * - Return the result of translating 'str' into ‘application/x-www-form-urlencoded’ format using the UTF-8 encoding scheme.
 */
@TransformFunction(names = {"url_encode"}, parameter = "(String str)", descriptions = {
        "- Return \"\" if 'str' is NULL, or there is an issue with the decoding process(such as encountering an illegal "
                +
                "escape pattern), or the encoding scheme is not supported;",
        "- Return the result of translating 'str' into 'application/x-www-form-urlencoded' format using the UTF-8 " +
                "encoding scheme."
}, examples = {
        "url_encode('https://apache.inlong.com/search?q=java url encode') = " +
                "\"https%3A%2F%2Fapache.inlong.com%2Fsearch%3Fq%3Djava+url+encode\""
})
public class UrlEncodeFunction implements ValueParser {

    private final ValueParser stringParser;

    public UrlEncodeFunction(Function expr) {
        stringParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj = stringParser.parse(sourceData, rowIndex, context);
        if (stringObj == null) {
            return null;
        }

        String string = OperatorTools.parseString(stringObj);
        if (string == null) {
            return null;
        }

        try {
            return URLEncoder.encode(string, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            return null;
        }
    }

}
