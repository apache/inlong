package org.apache.inlong.sdk.transform.process.function;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class RoundFunction implements ValueParser {
    private ValueParser numberParser;
    private ValueParser reservedDigitsParser;

    public RoundFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        numberParser = OperatorTools.buildParser(expressions.get(0));
        if(expressions.size() == 2) {
            reservedDigitsParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numberObj = numberParser.parse(sourceData, rowIndex, context);
        BigDecimal number = OperatorTools.parseBigDecimal(numberObj);
        if(reservedDigitsParser != null) {
            Object reservedDigitsObj = reservedDigitsParser.parse(sourceData, rowIndex, context);
            int reservedDigits = OperatorTools.parseBigDecimal(reservedDigitsObj).intValue();
            return number.setScale(reservedDigits, RoundingMode.HALF_UP).doubleValue();
        }
        return Math.round(number.doubleValue());
    }
}
