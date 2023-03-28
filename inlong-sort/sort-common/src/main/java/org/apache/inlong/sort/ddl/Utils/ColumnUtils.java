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
