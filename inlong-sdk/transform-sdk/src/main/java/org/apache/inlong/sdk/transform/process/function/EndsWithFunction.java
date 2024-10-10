package org.apache.inlong.sdk.transform.process.function;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * EndsWithFunction
 * description: endswith(expr, endExpr)
 * Returns whether expr ends with endExpr.
 */
@TransformFunction(names = {"endswith"})
public class EndsWithFunction implements ValueParser {

    private ValueParser exprParser;
    private ValueParser endExprParser;

    public EndsWithFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null && expressions.size() == 2) {
            exprParser = OperatorTools.buildParser(expressions.get(0));
            endExprParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object exprObj = exprParser.parse(sourceData, rowIndex, context);
        Object endExprObj = endExprParser.parse(sourceData, rowIndex, context);

        if (exprObj == null || endExprObj == null) {
            return null;
        }

        if (!isSameType(exprObj, endExprObj)) {
            throw new IllegalArgumentException("Both arguments must be of the same type.");
        }

        if (exprObj instanceof byte[] && endExprObj instanceof byte[]) {
            String exprString = new String((byte[]) exprObj);
            String endExprString = new String((byte[]) endExprObj);
            if (endExprString.isEmpty()) {
                return true;
            }

            return exprString.endsWith(endExprString);
        }

        String exprString = OperatorTools.parseString(exprObj);
        String endExprString = OperatorTools.parseString(endExprObj);

        if (endExprString.isEmpty()) {
            return true;
        }

        return exprString.endsWith(endExprString);
    }

    private boolean isSameType(Object a, Object b) {
        return (a instanceof String && b instanceof String) || (a instanceof byte[] && b instanceof byte[]);
    }
}

