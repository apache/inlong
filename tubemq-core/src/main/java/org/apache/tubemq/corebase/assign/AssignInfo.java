/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.assign;

import java.util.HashMap;
import java.util.Map;
import org.apache.tubemq.corebase.TokenConstants;

public class AssignInfo {
    private TupleType valueType =
            TupleType.TUPLE_VALUE_TYPE_OFFSET;
    private RangeType rangeType =
            RangeType.RANGE_SET_UNDEFINED;
    private ConflictSelect confSelect =
            ConflictSelect.CS_LEFT_OR_RIGHT_BIGGER;
    private Map<String, RangeTuple> partResetMap = new HashMap<>();
    private String partResetInfo = "";


    public AssignInfo() {

    }

    public AssignInfo(TupleType valueType,
                      RangeType rangeType,
                      ConflictSelect confSelect,
                      Map<String, RangeTuple> partResetMap) {
        this.valueType = valueType;
        this.rangeType = rangeType;
        this.confSelect = confSelect;
        this.partResetMap = partResetMap;
        buildResetInfo();
    }

    public void setAssignInfo(TupleType valueType,
                              RangeType rangeType,
                              ConflictSelect confSelect,
                              Map<String, RangeTuple> partResetMap) {
        this.valueType = valueType;
        this.rangeType = rangeType;
        this.confSelect = confSelect;
        this.partResetMap = partResetMap;
        buildResetInfo();
    }

    public TupleType getValueType() {
        return valueType;
    }

    public RangeType getRangeType() {
        return rangeType;
    }

    public ConflictSelect getConfSelect() {
        return confSelect;
    }

    public Map<String, RangeTuple> getPartResetMap() {
        return partResetMap;
    }

    public String getPartResetInfo() {
        return partResetInfo;
    }

    private void buildResetInfo() {
        int count = 0;
        Long leftValue = null;
        Long rightValue = null;
        StringBuilder sBuilder = new StringBuilder(512);
        for (Map.Entry<String, RangeTuple> entry : partResetMap.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                leftValue = entry.getValue().getLeftValue();
                rightValue = entry.getValue().getRightValue();
                if (count++ > 0) {
                    sBuilder.append(TokenConstants.ARRAY_SEP);
                }
                sBuilder.append(entry.getKey().trim())
                        .append(TokenConstants.EQ)
                        .append(leftValue == null ? -1 : leftValue)
                        .append(TokenConstants.ATTR_SEP)
                        .append(rightValue == null ? -1 : rightValue);
            }
        }
        this.partResetInfo = sBuilder.toString();
    }
}
