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
import org.apache.inlong.sdk.transform.process.function.compression.factory.CompressionAlgorithmFactory;
import org.apache.inlong.sdk.transform.process.function.compression.handler.CompressHandler;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * CompressFunction  ->  compress(string_to_compress[,compress_type(default:deflater)])
 * description:
 * - Return NULL if 'string_to_compress' is NULL
 * - Return "" if 'string_to_compress' is ""
 * - Return the result as a binary string
 * Note: This function supports three compression algorithms: deflater, gzip and zip. 'compress_type' defaults to defer.
 * In addition, in order to output the compressed results in the form of strings, this method uses the ISO_8859_1
 * character set.
 */
@Slf4j
@TransformFunction(type = FunctionConstant.COMPRESSION_TYPE, names = {
        "compress"}, parameter = "(String string_to_compress [, String compress_type])", descriptions = {
                "- Return \"\" if 'string_to_compress' is NULL;", "- Return \"\" if 'string_to_compress' is \"\";",
                "- Return the result as a binary string.",
                "Note: This function supports three compression algorithms: deflater, gzip, and zip. 'compress_type' defaults to defer. In addition, in order to output the compressed results in the form of strings, this method uses the ISO_8859_1 character set."}, examples = {
                        "length(compress(replicate(string1,100)),'ISO_8859_1') = 33",
                        "length(compress(''),'ISO_8859_1') = 0"})
public class CompressFunction implements ValueParser {

    private final ValueParser stringParser;
    private final ValueParser compressTypeParser;

    // ISO-8859-1 encoding does not convert negative numbers and can preserve the original values of byte arrays.
    private final Charset CHARSET = StandardCharsets.ISO_8859_1;
    private final String DEFAULT_COMPRESS_TYPE = "deflater";

    public CompressFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() == 2) {
            compressTypeParser = OperatorTools.buildParser(expressions.get(1));
        } else {
            compressTypeParser = null;
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObject = stringParser.parse(sourceData, rowIndex, context);
        if (stringObject == null) {
            return null;
        }
        String str = OperatorTools.parseString(stringObject);
        if (str.isEmpty()) {
            return "";
        }

        // parse compress type
        String compressType = DEFAULT_COMPRESS_TYPE;
        if (compressTypeParser != null) {
            Object compressTypeObj = compressTypeParser.parse(sourceData, rowIndex, context);
            if (compressTypeObj != null) {
                compressType = OperatorTools.parseString(compressTypeObj);
            }
        }
        compressType = compressType.toLowerCase();

        try {
            byte[] lengthBytes = intToLowByteArray(str.length());
            CompressHandler handler = CompressionAlgorithmFactory.getCompressHandlerByName(compressType);
            if (handler == null) {
                throw new RuntimeException(compressType + " is not supported.");
            }
            byte[] compressBytes = handler.compress(str.getBytes(CHARSET));
            return mergeByteArray(lengthBytes, compressBytes);
        } catch (Exception e) {
            log.error("Compression failed", e);
            return null;
        }
    }

    private String mergeByteArray(byte[] lengthBytes, byte[] dateBytes) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(
                lengthBytes.length + dateBytes.length);
        outputStream.write(lengthBytes);
        outputStream.write(dateBytes);
        return new String(outputStream.toByteArray(), CHARSET);
    }

    // low byte first
    public static byte[] intToLowByteArray(int length) {
        return new byte[]{
                (byte) (length & 0xFF),
                (byte) ((length >> 8) & 0xFF),
                (byte) ((length >> 16) & 0xFF),
                (byte) ((length >> 24) & 0xFF)
        };
    }
}