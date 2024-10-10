package org.apache.inlong.sdk.transform.process.function;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * StartsWithFunction
 * description: startswith(expr, startExpr)
 * Returns whether expr starts with startExpr.
 */
@TransformFunction(names = {"startswith"})
public class StartsWithFunction implements ValueParser {

    private ValueParser exprParser;
    private ValueParser startExprParser;

    public StartsWithFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null && expressions.size() == 2) {
            exprParser = OperatorTools.buildParser(expressions.get(0));
            startExprParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object exprObj = exprParser.parse(sourceData, rowIndex, context);
        Object startExprObj = startExprParser.parse(sourceData, rowIndex, context);

        if (exprObj == null || startExprObj == null) {
            return null;
        }

        if (!isSameType(exprObj, startExprObj)) {
            throw new IllegalArgumentException("Both arguments must be of the same type.");
        }

        if (exprObj instanceof byte[] && startExprObj instanceof byte[]) {
            String exprString = new String((byte[]) exprObj);
            String startExprString = new String((byte[]) startExprObj);
            if (startExprString.isEmpty()) {
                return true;
            }

            return exprString.startsWith(startExprString);
        }

        String exprString = OperatorTools.parseString(exprObj);
        String startExprString = OperatorTools.parseString(startExprObj);

        if (startExprString.isEmpty()) {
            return true;
        }

        return exprString.startsWith(startExprString);
    }

    private boolean isSameType(Object a, Object b) {
        return (a instanceof String && b instanceof String) || (a instanceof byte[] && b instanceof byte[]);
    }
}
