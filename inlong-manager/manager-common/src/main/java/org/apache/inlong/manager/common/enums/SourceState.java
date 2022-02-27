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

package org.apache.inlong.manager.common.enums;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

public enum SourceState {

    SOURCE_ADD(100, "start source collecting"),
    SOURCE_DEL(101, "delete source collecting"),
    SOURCE_FROZEN(104, "stop source collecting"),
    SOURCE_ACTIVE(105, "active source collecting");

    private static final Map<SourceState, Set<SourceState>> SOURCE_FINITE_STATE_AUTOMATON = Maps.newHashMap();

    static {
        SOURCE_FINITE_STATE_AUTOMATON.put(SOURCE_ADD,
                Sets.newHashSet(SOURCE_ADD, SOURCE_DEL, SOURCE_FROZEN));
        SOURCE_FINITE_STATE_AUTOMATON.put(SOURCE_FROZEN,
                Sets.newHashSet(SOURCE_FROZEN, SOURCE_ACTIVE, SOURCE_DEL));
        SOURCE_FINITE_STATE_AUTOMATON.put(SOURCE_ACTIVE,
                Sets.newHashSet(SOURCE_ACTIVE, SOURCE_FROZEN, SOURCE_DEL));
        SOURCE_FINITE_STATE_AUTOMATON.put(SOURCE_DEL,
                Sets.newHashSet(SOURCE_DEL));
    }

    public static SourceState forCode(int code) {
        for (SourceState state : values()) {
            if (state.getCode() == code) {
                return state;
            }
        }
        throw new IllegalStateException(String.format("Illegal code=%s for SourceState", code));
    }

    public static boolean isAllowedTransition(SourceState pre, SourceState now) {
        Set<SourceState> nextStates = SOURCE_FINITE_STATE_AUTOMATON.get(pre);
        return nextStates != null && nextStates.contains(now);
    }

    private final Integer code;
    private final String description;

    SourceState(Integer code, String description) {
        this.code = code;
        this.description = description;
    }

    public Integer getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
