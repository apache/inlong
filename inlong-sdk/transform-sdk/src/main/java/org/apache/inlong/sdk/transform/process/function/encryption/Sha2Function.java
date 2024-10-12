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
import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;

import static org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_224;

/**
 * Sha2Function  ->  SHA2(str, hash_length)
 * description:
 * - Return NULL if either argument is NULL or the hash_length(224 256 384 512) is not one of the permitted values
 * - Return a hash value containing the 'hash_length' of bits
 */
@TransformFunction(type = FunctionConstant.ENCRYPTION_TYPE, names = {
        "sha2"}, parameter = "(String str, Integer hash_length)", descriptions = {
                "- Return \"\" if either argument is NULL or the 'hash_length' is not one of (224,256,384,512);",
                "- Return scale of the argument (the number of decimal digits in the fractional part)."}, examples = {
                        "sha2(\"5\",224) = \"b51d18b551043c1f145f22dbde6f8531faeaf68c54ed9dd79ce24d17\""})
public class Sha2Function implements ValueParser {

    private final ValueParser msgParser;
    private final ValueParser lenParser;

    public Sha2Function(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        msgParser = OperatorTools.buildParser(expressions.get(0));
        lenParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object msgObj = msgParser.parse(sourceData, rowIndex, context);
        Object lenObj = lenParser.parse(sourceData, rowIndex, context);
        if (msgObj == null || lenObj == null) {
            return null;
        }
        byte[] msgBytes = OperatorTools.parseBytes(msgObj);
        int len = Integer.parseInt(lenObj.toString());
        switch (len) {
            case 0:
            case 256:
                return DigestUtils.sha256Hex(msgBytes);
            case 224:
                return new DigestUtils(SHA_224).digestAsHex(msgBytes);
            case 384:
                return DigestUtils.sha384Hex(msgBytes);
            case 512:
                return DigestUtils.sha512Hex(msgBytes);
            default:
                return null;
        }
    }
}
