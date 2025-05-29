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

package org.apache.inlong.sdk.transform.decode;

import java.util.List;
import java.util.Map;

public class YamlSourceData extends AbstractSourceData {

    public static final String ROOT_KEY = "$root";

    public static final String CHILD_KEY = "$child";

    private YamlNode root;

    private YamlNode childRoot;

    public YamlSourceData(YamlNode root, YamlNode childRoot) {
        this.root = root;
        this.childRoot = childRoot;
    }
    @Override
    public int getRowCount() {
        if (this.childRoot == null) {
            return 1;
        } else {
            Object value = this.childRoot.getValue();
            if (value instanceof List) {
                return ((List<YamlNode>) value).size();
            } else {
                return 1;
            }
        }
    }

    @Override
    public String getField(int rowNum, String fieldName) {
        try {
            String[] nodeString = fieldName.split("\\.");
            Object cur = null, last = null;
            int start = -1;

            if (nodeString[0].equals(ROOT_KEY)) {
                cur = root;
            } else if (nodeString[0].equals(CHILD_KEY)) {
                cur = ((List<YamlNode>) childRoot.getValue()).get(rowNum);
            }

            for (int i = 1; i < nodeString.length; i++) {
                if (cur == null) {
                    cur = last;
                    continue;
                }
                last = cur;
                if (cur instanceof List) {
                    int idx = 0;
                    start = nodeString[i].indexOf('(');
                    if (start != -1) {
                        idx = Integer.parseInt(nodeString[1].substring(start + 1, nodeString[1].indexOf(')')));
                    }
                    cur = ((List<?>) cur).get(idx);
                } else if (cur instanceof Map) {
                    start = nodeString[i].indexOf('(');
                    String key = nodeString[i];
                    if (start != -1) {
                        key = key.substring(0, start);
                    }
                    cur = ((Map<String, YamlNode>) cur).get(key);
                } else if (cur instanceof YamlNode) {
                    cur = ((YamlNode) cur).getValue();
                } else {
                    i++;
                }
                i--;
            }
            if (cur == null) {
                return "";
            }
            return cur.toString();
        } catch (Exception e) {
            return "";
        }
    }
}
