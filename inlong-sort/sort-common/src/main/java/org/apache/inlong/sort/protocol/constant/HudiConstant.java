/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.inlong.sort.protocol.constant;

/**
 * Hudi option constant
 */
public class HudiConstant {
    /**
     * Hudi supported table type
     */
    public enum TableType {
        /**
         * creates a MERGE_ON_READ table.
         */
        MERGE_ON_READ,
        /**
         * creates a COPY_ON_WRITE table(default).
         */
        COPY_ON_WRITE;

        /**
         * get TableType from name
         */
        public static TableType forName(String name) {
            for (TableType value : values()) {
                if (value.name().equals(name)) {
                    return value;
                }
            }
            throw new IllegalArgumentException(String.format("Unsupport TableType:%s", name));
        }
    }
}