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

import lombok.Data;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * JsonNode
 * 
 */
@Data
public class JsonNode {

    private String name;
    private boolean isArray = false;
    private int arrayIndex = -1;

    public JsonNode(String nodeString) {
        int beginIndex = nodeString.indexOf('[');
        if (beginIndex < 0) {
            this.name = nodeString;
        } else {
            this.name = StringUtils.trim(nodeString.substring(0, beginIndex));
            int endIndex = nodeString.lastIndexOf(']');
            if (endIndex >= 0) {
                this.isArray = true;
                this.arrayIndex = NumberUtils.toInt(nodeString.substring(beginIndex + 1, endIndex), -1);
                if (this.arrayIndex < 0) {
                    this.arrayIndex = 0;
                }
            }
        }
    }
}
