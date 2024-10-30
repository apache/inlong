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

import net.sf.jsqlparser.expression.Function;

import java.nio.charset.StandardCharsets;

/**
 * SoundexFunction  ->  soundex(str)
 * description:
 * - Return NULL if 'str' is NULL
 * - Return a four character code representing the sound of 'str'
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "soundex"}, parameter = "(String str)", descriptions = {
                "- Return \"\" if 'str' is NULL;",
                "- Return a four character code representing the sound of 'str'."
        }, examples = {"soundex('hello world') = \"H464\""})
public class SoundexFunction implements ValueParser {

    private ValueParser stringParser;

    private static final byte[] SOUNDEX_INDEX =
            "71237128722455712623718272\000\000\000\000\000\00071237128722455712623718272"
                    .getBytes(StandardCharsets.ISO_8859_1);

    public SoundexFunction(Function expr) {
        stringParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObject = stringParser.parse(sourceData, rowIndex, context);
        String str = OperatorTools.parseString(stringObject);
        if (str == null) {
            return null;
        }
        return new String(getSoundex(str), StandardCharsets.ISO_8859_1);
    }

    private static byte[] getSoundex(String str) {
        byte[] chars = {'0', '0', '0', '0'};
        byte lastDigit = '0';
        for (int i = 0, j = 0, l = str.length(); i < l && j < 4; i++) {
            char c = str.charAt(i);
            if (c >= 'A' && c <= 'z') {
                byte newDigit = SOUNDEX_INDEX[c - 'A'];
                if (newDigit != 0) {
                    if (j == 0) {
                        chars[j++] = (byte) (c & 0xdf); // Converts a-z to A-Z
                        lastDigit = newDigit;
                    } else if (newDigit <= '6') {
                        if (newDigit != lastDigit) {
                            chars[j++] = lastDigit = newDigit;
                        }
                    } else if (newDigit == '7') {
                        lastDigit = newDigit;
                    }
                }
            }
        }
        return chars;
    }
}
