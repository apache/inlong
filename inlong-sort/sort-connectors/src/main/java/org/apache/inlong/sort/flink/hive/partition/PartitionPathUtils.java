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

package org.apache.inlong.sort.flink.hive.partition;

import java.util.BitSet;
import org.apache.flink.core.fs.Path;

public class PartitionPathUtils {

    private static final BitSet CHAR_TO_ESCAPE = new BitSet(128);

    /**
     * Make partition path from partition spec.
     *
     * @param hivePartition The partition spec.
     * @return An escaped, valid partition name.
     */
    public static String generatePartitionPath(HivePartition hivePartition) {
        if (hivePartition.getPartitions().length == 0) {
            return "";
        }
        StringBuilder suffixBuf = new StringBuilder();
        for (int i = 0; i < hivePartition.getPartitions().length; i++) {
            suffixBuf.append(escapePathName(hivePartition.getPartitions()[i].f0));
            suffixBuf.append('=');
            suffixBuf.append(escapePathName(hivePartition.getPartitions()[i].f1));
            suffixBuf.append(Path.SEPARATOR);
        }
        return suffixBuf.toString();
    }

    /**
     * Escapes a path name.
     *
     * @param path The path to escape.
     * @return An escaped path name.
     */
    private static String escapePathName(String path) {
        if (path == null || path.length() == 0) {
            throw new RuntimeException("Path should not be null or empty: " + path);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (needsEscaping(c)) {
                sb.append('%');
                sb.append(String.format("%1$02X", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static boolean needsEscaping(char c) {
        return c < CHAR_TO_ESCAPE.size() && CHAR_TO_ESCAPE.get(c);
    }
}
