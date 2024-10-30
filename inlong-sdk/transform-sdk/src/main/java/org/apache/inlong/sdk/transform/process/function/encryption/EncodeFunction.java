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

package org.apache.inlong.sdk.transform.process.function.encryption;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * EncodeFunction  ->  encode(strInfo, charsetStr)
 * Description:
 * - Return NULL if any parameter is NULL
 * - Return the result of encoding strInfo using the character set specified by charsetStr
 * Note: charsetStr is one of ('US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
 */
@TransformFunction(type = FunctionConstant.ENCRYPTION_TYPE, names = {
        "encode"}, parameter = "(String strInfo,String charsetStr)", descriptions = {
                "- Return \"\" if any parameter is NULL;",
                "- Return the result of encoding 'strInfo' using the character set specified by 'charsetStr'.",
                "- Note: 'charsetStr' is one of ('US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16')."
        }, examples = {"decode(encode('Hello','UTF-8'),'UTF-8') = \"Hello\""})
public class EncodeFunction implements ValueParser {

    private ValueParser stringParser;

    private ValueParser characterSetParser;

    private static final Set<String> SUPPORTED_CHARSETS;

    static {
        Set<String> charsets = new HashSet<>();
        charsets.add(StandardCharsets.US_ASCII.name());
        charsets.add(StandardCharsets.ISO_8859_1.name());
        charsets.add(StandardCharsets.UTF_8.name());
        charsets.add(StandardCharsets.UTF_16.name());
        charsets.add(StandardCharsets.UTF_16BE.name());
        charsets.add(StandardCharsets.UTF_16LE.name());
        SUPPORTED_CHARSETS = Collections.unmodifiableSet(charsets);
    }

    public EncodeFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null && expressions.size() == 2) {
            stringParser = OperatorTools.buildParser(expressions.get(0));
            characterSetParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj = stringParser.parse(sourceData, rowIndex, context);
        Object characterObj = characterSetParser.parse(sourceData, rowIndex, context);
        if (stringObj == null || characterObj == null) {
            return null;
        }
        String stringValue = OperatorTools.parseString(stringObj);
        String characterSetValue = OperatorTools.parseString(characterObj).toUpperCase();
        return encode(stringValue, characterSetValue);
    }

    private byte[] encode(String stringValue, String characterSetValue) {
        if (stringValue == null || stringValue.isEmpty() || characterSetValue == null || characterSetValue.isEmpty()) {
            return new byte[0];
        }
        if (Charset.isSupported(characterSetValue) && SUPPORTED_CHARSETS.contains(characterSetValue)) {
            Charset charset = Charset.forName(characterSetValue);
            return stringValue.getBytes(charset);
        }
        return null;
    }
}
