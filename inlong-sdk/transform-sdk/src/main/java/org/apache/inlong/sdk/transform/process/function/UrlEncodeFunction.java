package org.apache.inlong.sdk.transform.process.function;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;

/**
 * UrlEncodeFunction
 * description: Translates a string into ‘application/x-www-form-urlencoded’ format using the UTF-8 encoding scheme.
 * If the input is NULL, or there is an issue with the encoding process,
 * or the encoding scheme is not supported, will return NULL.
 */
@TransformFunction(names = {"url_encode"})
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

    public static void main(String[] args) throws UnsupportedEncodingException {
        String string = "https://www.google.com/search?q=java url encode";
        String s = "bat apache";
        System.out.println(URLEncoder.encode(string, StandardCharsets.UTF_8.toString()));
        System.out.println(URLEncoder.encode(s, StandardCharsets.UTF_8.toString()));
    }
}

