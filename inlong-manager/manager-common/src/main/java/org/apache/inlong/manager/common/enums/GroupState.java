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
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public enum GroupState {
    // Inlong group related status
    GROUP_DRAFT(0, "draft"),
    GROUP_WAIT_SUBMIT(100, "waiting for submit"),
    GROUP_WAIT_APPROVAL(101, "waiting for approval"),
    GROUP_APPROVE_REJECTED(102, "approval rejected"),
    GROUP_APPROVE_PASSED(103, "approval passed"),
    GROUP_CONFIG_ING(110, "in configure"),
    GROUP_CONFIG_FAILED(120, "configuration failed"),
    GROUP_CONFIG_SUCCESSFUL(130, "configuration successful"),
    GROUP_SUSPEND_ING(141, "suspending"),
    GROUP_SUSPEND(140, "suspend"),
    GROUP_RESTART_ING(151, "restart"),
    GROUP_RESTART(150, "restart"),
    GROUP_DELETE_ING(41, "deleting"),
    GROUP_DELETE(40, "delete"),
    //GROUP_FINISH is used for batch task.
    GROUP_FINISH(131, "finish");

    private static final Map<GroupState, Set<GroupState>> GROUP_FINITE_STATE_AUTOMATON = Maps.newHashMap();

    /**
     * Init group finite state automaton
     */
    static {
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_DRAFT,
                Sets.newHashSet(GROUP_DRAFT, GROUP_WAIT_SUBMIT, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_WAIT_SUBMIT,
                Sets.newHashSet(GROUP_WAIT_SUBMIT, GROUP_WAIT_APPROVAL, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_WAIT_APPROVAL,
                Sets.newHashSet(GROUP_WAIT_APPROVAL, GROUP_APPROVE_REJECTED, GROUP_APPROVE_PASSED));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_APPROVE_REJECTED,
                Sets.newHashSet(GROUP_APPROVE_REJECTED, GROUP_WAIT_APPROVAL, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_APPROVE_PASSED,
                Sets.newHashSet(GROUP_APPROVE_PASSED, GROUP_CONFIG_ING, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_CONFIG_ING,
                Sets.newHashSet(GROUP_CONFIG_ING, GROUP_CONFIG_FAILED, GROUP_CONFIG_SUCCESSFUL));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_CONFIG_FAILED,
                Sets.newHashSet(GROUP_CONFIG_FAILED, GROUP_WAIT_APPROVAL, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_CONFIG_SUCCESSFUL,
                Sets.newHashSet(GROUP_CONFIG_SUCCESSFUL, GROUP_WAIT_APPROVAL, GROUP_SUSPEND_ING, GROUP_FINISH,
                        GROUP_DELETE_ING, GROUP_SUSPEND));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_SUSPEND_ING,
                Sets.newHashSet(GROUP_SUSPEND_ING, GROUP_SUSPEND));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_SUSPEND,
                Sets.newHashSet(GROUP_SUSPEND, GROUP_RESTART_ING, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_RESTART_ING,
                Sets.newHashSet(GROUP_RESTART_ING, GROUP_RESTART));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_RESTART,
                Sets.newHashSet(GROUP_RESTART, GROUP_SUSPEND_ING, GROUP_WAIT_APPROVAL, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_FINISH,
                Sets.newHashSet(GROUP_FINISH, GROUP_DELETE_ING));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_DELETE_ING,
                Sets.newHashSet(GROUP_DELETE_ING, GROUP_DELETE));
        GROUP_FINITE_STATE_AUTOMATON.put(GROUP_DELETE,
                Sets.newHashSet(GROUP_DELETE));
    }

    public static GroupState forCode(int code) {
        for (GroupState state : values()) {
            if (state.getCode() == code) {
                return state;
            }
        }
        throw new IllegalStateException(String.format("Illegal code=%s for GroupState", code));
    }

    public static boolean isAllowedTransition(GroupState pre, GroupState now) {
        Set<GroupState> nextStates = GROUP_FINITE_STATE_AUTOMATON.get(pre);
        return nextStates != null && nextStates.contains(now);
    }

    public static boolean isAllowedUpdate(GroupState state) {
        return state != GroupState.GROUP_CONFIG_ING
                && state != GroupState.GROUP_WAIT_APPROVAL;
    }

    public static boolean isAllowedLogicDel(GroupState state) {
        return state == GroupState.GROUP_DRAFT || state == GroupState.GROUP_WAIT_SUBMIT;
    }

    private final Integer code;
    private final String description;

    GroupState(Integer code, String description) {
        this.code = code;
        this.description = description;
    }

    public Integer getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT).replace("group_", "");
    }
}
