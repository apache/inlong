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

package org.apache.inlong.common.enums;

import com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Set;

public enum InlongCompressType {

    NONE(0, "NONE", "The message compressed with nothing"),
    INLONG_GZ(1, "INLONG_GZ", "The message compressed with inlong gz"),
    INLONG_SNAPPY(2, "INLONG_SNAPPY", "The message compressed with inlong snappy"),
    UNKNOWN(99, "UNKNOWN", "Unknown compress type");

    public static final Set<String> allowedCompressTypes =
            Sets.newHashSet(NONE.getName(), INLONG_GZ.getName(), INLONG_SNAPPY.getName());

    private final int id;
    private final String name;
    private final String desc;

    InlongCompressType(int id, String name, String desc) {
        this.id = id;
        this.name = name;
        this.desc = desc;
    }

    public static InlongCompressType valueOf(int value) {
        for (InlongCompressType msgCompressType : InlongCompressType.values()) {
            if (msgCompressType.getId() == value) {
                return msgCompressType;
            }
        }
        return UNKNOWN;
    }

    public static InlongCompressType forType(String type) {
        for (InlongCompressType msgCompressType : InlongCompressType.values()) {
            if (Objects.equals(msgCompressType.getName(), type)) {
                return msgCompressType;
            }
        }
        return UNKNOWN;
    }

    public int getId() {
        return id;
    }

    public String getStrId() {
        return String.valueOf(id);
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }
}
