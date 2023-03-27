package org.apache.inlong.sort.ddl.Utils;

import java.util.List;
import org.apache.inlong.sort.ddl.Position;
import org.apache.inlong.sort.ddl.enums.PositionType;


public class ColumnUtils {

    public static String getDefaultValue(List<String> specs) {
        if (specs == null) {
            return null;
        }
        int index = getIndexOfSpecificString(specs, "DEFAULT");
        return index != -1 ? specs.get(index + 1) : null;
    }

    public static boolean getNullable(List<String> specs) {
        if (specs == null) {
            return true;
        }
        int index = getIndexOfSpecificString(specs, "null");
        return index <= 0 || !specs.get(index - 1).equalsIgnoreCase("not");
    }

    public static String getComment(List<String> specs) {
        if (specs == null) {
            return null;
        }
        int index = getIndexOfSpecificString(specs, "COMMENT");
        return index != -1 ? specs.get(index + 1) : null;
    }

    public static Position getPosition(List<String> specs) {
        if (specs == null) {
            return null;
        }
        int index = getIndexOfSpecificString(specs, "AFTER");
        return index != -1 ? new Position(PositionType.AFTER, specs.get(index + 1)) : null;
    }


    public static int getIndexOfSpecificString(List<String> specs, String specificString) {
        for (int i = 0; i < specs.size(); i++) {
            if (specs.get(i).equalsIgnoreCase(specificString)) {
                return i;
            }
        }
        return -1;
    }

}
