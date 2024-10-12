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

package org.apache.inlong.sdk.transform.process.function.compression;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.function.compression.factory.UnCompressionAlgorithmFactory;
import org.apache.inlong.sdk.transform.process.function.compression.handler.UncompressHandler;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * UnCompressFunction -> uncompress(string_to_uncompress [,compress_type])
 * description:
 * - Return NULL if 'string_to_uncompress' is NULL;
 * - Return "" if 'string_to_uncompress' is "";
 * - Return the result as a string.
 * Note: This function supports three compression algorithms: deflater, gzip, and zip. compress_type defaults to defer.
 */
@Slf4j
@TransformFunction(type = FunctionConstant.COMPRESSION_TYPE, names = {
        "uncompress"}, parameter = "(String string_to_uncompress, String compress_type)", descriptions = {
                "- Return \"\" if 'string_to_uncompress' is NULL;", "- Return \"\" if 'string_to_uncompress' is \"\";",
                "- Return the result as a string.",
                "Note: This function supports three compression algorithms: deflater, gzip and zip. 'compress_type' defaults to defer."}, examples = {
                        "uncompress(compress('inlong')) = \"inlong\""})
public class UnCompressFunction implements ValueParser {

    private final ValueParser stringParser;
    private final ValueParser uncompressTypeParser;

    // ISO-8859-1 encoding does not convert negative numbers and can preserve the original values of byte arrays.
    private final Charset CHARSET = StandardCharsets.ISO_8859_1;
    private final String DEFAULT_UNCOMPRESS_TYPE = "deflater";

    public UnCompressFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() == 2) {
            uncompressTypeParser = OperatorTools.buildParser(expressions.get(1));
        } else {
            uncompressTypeParser = null;
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        // parse data
        Object stringObject = stringParser.parse(sourceData, rowIndex, context);
        if (stringObject == null) {
            return null;
        }
        String str = OperatorTools.parseString(stringObject);
        if (str.isEmpty()) {
            return "";
        } else if (str.startsWith("null")) {
            return null;
        }
        // parse uncompress type
        String uncompressType = DEFAULT_UNCOMPRESS_TYPE;
        if (uncompressTypeParser != null) {
            Object compressTypeObj = uncompressTypeParser.parse(sourceData, rowIndex, context);
            if (compressTypeObj != null) {
                uncompressType = OperatorTools.parseString(compressTypeObj);
            }
        }
        uncompressType = uncompressType.toLowerCase();
        // uncompress
        try {
            // The first four bytes are the data length
            byte[] compressData = Arrays.copyOfRange(str.getBytes(CHARSET), 4, str.length());
            UncompressHandler handler = UnCompressionAlgorithmFactory.getCompressHandlerByName(uncompressType);
            if (handler == null) {
                throw new RuntimeException(uncompressType + " is not supported.");
            }
            return new String(handler.uncompress(compressData));
        } catch (Exception e) {
            log.error("UnCompression failed", e);
            return null;
        }
    }
}