package org.apache.inlong.sort.doris.utils;

import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.inlong.common.metric.MetricObserver.LOG;

public class DorisParseUtils {

    public static Map<String, String> parsetoMap(Object data) {
        String[] toParse = data.toString().split("\\s+");
        Map<String, String> ret = new HashMap<>();
        if (toParse.length < 2) {
            LOG.warn("parse length insufficient! string is :{}", Arrays.toString(toParse));
            return ret;
        }
        ret.put("id", toParse[0]);
        ret.put("__DORIS_DELETE_SIGN__", toParse[1]);
        return ret;
    }

    public static String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new RuntimeException("Unrecognized row kind:" + rowKind.toString());
        }
    }

    public static String escapeString(String s) {
        Pattern p = Pattern.compile("\\\\x(\\d{2})");
        Matcher m = p.matcher(s);

        StringBuffer buf = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(buf, String.format("%s", (char) Integer.parseInt(m.group(1))));
        }
        m.appendTail(buf);
        return buf.toString();
    }

}
