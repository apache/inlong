package org.apache.inlong.sdk.transform.process.function;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import java.util.ArrayList;
import java.util.List;
@TransformFunction(names = {"printf"})
public class PrintfFunction implements ValueParser {
    private ValueParser strfmtParser;
    private List<ValueParser> argsParser = new ArrayList<>();
    public PrintfFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        strfmtParser = OperatorTools.buildParser(expressions.get(0));
        for (int i = 1 ; i < expressions.size() ; i ++) {
            argsParser.add(OperatorTools.buildParser(expressions.get(i)));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object strfmtObj = strfmtParser.parse(sourceData, rowIndex, context);
        String strfmt = OperatorTools.parseString(strfmtObj);
        int size = argsParser.size();
        Object[] args = new Object[size];
        for (int i = 0 ; i < size ; i ++) {
            Object arg = argsParser.get(i).parse(sourceData, rowIndex, context).toString();
            String argStr = arg.toString();
            if (isInteger(argStr)) {
                arg = Integer.parseInt(argStr);
            } else if (isFloat(argStr)) {
                arg = Float.parseFloat(argStr);
            }
            args[i] = arg;
        }
        return String.format(strfmt, args);
    }

    public boolean isFloat(String str) {
        return str.matches("^(-?\\d+)(\\.\\d+)?$");
    }

    public boolean isInteger(String str){
        return str.matches("-?\\\\d+");
    }

}
