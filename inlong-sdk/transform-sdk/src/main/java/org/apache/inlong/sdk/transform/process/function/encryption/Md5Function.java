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
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;

/**
 * Md5Function  ->  MD5(string)
 * description:
 * - Return NULL if the string is NULL
 * - Return the MD5 hash value of a string in the form of a 32-bit hexadecimal digit string
 */
@TransformFunction(names = {"md5"}, parameter = "(String string)", descriptions = {
        "- Return \"\" if the 'string' is NULL;",
        "- Return the MD5 hash value of 'string' in the form of a 32-bit hexadecimal digit string."
}, examples = {
        "md5(\"\") = \"d41d8cd98f00b204e9800998ecf8427e\"",
        "md5(\"1\") = \"c4ca4238a0b923820dcc509a6f75849b\""
})
public class Md5Function implements ValueParser {

    private ValueParser msgParser;

    public Md5Function(Function expr) {
        msgParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object msgObj = msgParser.parse(sourceData, rowIndex, context);
        if (msgObj == null) {
            return null;
        }
        String msg = msgObj.toString();
        return DigestUtils.md5Hex(msg.getBytes(StandardCharsets.UTF_8));
    }
}
