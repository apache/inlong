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


public class AssignUtils {

    public static boolean isOffsetReseted(AssignInfo assignInfo) {
        return ((assignInfo != null)
            && (assignInfo.getRangeType() != RangeType.RANGE_SET_UNDEFINED));
    }

    public static void checkResetValue(String partKey,
                                       RangeType rangeType,
                                       RangeTuple tuple) throws Exception {
        if (tuple == null) {
            throw new Exception(new StringBuilder(256)
                .append("tuple is null for ").append(rangeType.getToken())
                .append(" type, partitionKey = ")
                .append(partKey).toString());
        }
        switch (rangeType) {
            case RANGE_SET_LEFT_DEFINED: {
                if (tuple.getLeftValue() != null) {
                    if (tuple.getLeftValue() < 0) {
                        throw new Exception(new StringBuilder(256)
                            .append("left-value must over or equal zero for ")
                            .append(rangeType.getToken()).append(" type, partitionKey = ")
                            .append(partKey).append(", left-value=").append(tuple.getLeftValue())
                            .toString());
                    }
                }
                break;
            }
            case RANGE_SET_RIGHT_DEFINED: {
                if (tuple.getRightValue() == null) {
                    throw new Exception(new StringBuilder(256)
                        .append("right-value must set for ")
                        .append(rangeType.getToken()).append(" type, partitionKey = ")
                        .append(partKey).toString());
                }
                if (tuple.getRightValue() < 0) {
                    throw new Exception(new StringBuilder(256)
                        .append("right-value must over or equal zero for ")
                        .append(rangeType.getToken()).append(" type, partitionKey = ")
                        .append(partKey).append(", right-value=").append(tuple.getRightValue())
                        .toString());
                }
                break;
            }
            case RANGE_SET_BOTH_DEFINED: {
                if (tuple.getLeftValue() == null
                    || tuple.getRightValue() == null) {
                    throw new Exception(new StringBuilder(256)
                        .append("left-value and right-value must set for ")
                        .append(rangeType.getToken()).append(" type, partitionKey = ")
                        .append(partKey).toString());
                }
                if (tuple.getLeftValue() < 0) {
                    throw new Exception(new StringBuilder(256)
                        .append("left-value must over or equal zero for ")
                        .append(rangeType.getToken()).append(" type, partitionKey = ")
                        .append(partKey).append(", left-value=").append(tuple.getLeftValue())
                        .toString());
                }
                if (tuple.getRightValue() < 0) {
                    throw new Exception(new StringBuilder(256)
                        .append("right-value must over or equal zero for ")
                        .append(rangeType.getToken()).append(" type, partitionKey = ")
                        .append(partKey).append(", right-value=").append(tuple.getRightValue())
                        .toString());
                }
                if (tuple.getLeftValue() > tuple.getRightValue()) {
                    throw new Exception(new StringBuilder(256)
                        .append("left-value must less than or equal to right-value for ")
                        .append(rangeType.getToken()).append(" type, partitionKey = ")
                        .append(partKey).append(", left-value=").append(tuple.getLeftValue())
                        .append(", right-value=").append(tuple.getRightValue())
                        .toString());
                }
                break;
            }
            default: {
                break;
            }
        }
    }

}
