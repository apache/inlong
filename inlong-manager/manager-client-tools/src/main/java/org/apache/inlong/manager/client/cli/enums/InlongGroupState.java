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

package org.apache.inlong.manager.client.cli.enums;

import org.apache.inlong.manager.common.enums.GroupStatus;

import java.util.ArrayList;
import java.util.List;

public enum InlongGroupState {
    CREATE, REJECTED, INITIALIZING, OPERATING, STARTED, FAILED, STOPPED, FINISHED, DELETED;

    public static List<Integer> parseStatus(String state) {

        InlongGroupState groupState;
        try {
            groupState = InlongGroupState.valueOf(state);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Unsupported status %s for group", state));
        }

        List<Integer> stateList = new ArrayList<>();
        switch (groupState) {
            case CREATE:
                stateList.add(GroupStatus.DRAFT.getCode());
                return stateList;
            case OPERATING:
                stateList.add(GroupStatus.DELETING.getCode());
                stateList.add(GroupStatus.SUSPENDING.getCode());
                stateList.add(GroupStatus.RESTARTING.getCode());
                return stateList;
            case REJECTED:
                stateList.add(GroupStatus.APPROVE_REJECTED.getCode());
                return stateList;
            case INITIALIZING:
                stateList.add(GroupStatus.TO_BE_APPROVAL.getCode());
                stateList.add(GroupStatus.APPROVE_PASSED.getCode());
                stateList.add(GroupStatus.CONFIG_ING.getCode());
                return stateList;
            case FAILED:
                stateList.add(GroupStatus.CONFIG_FAILED.getCode());
                return stateList;
            case STARTED:
                stateList.add(GroupStatus.RESTARTED.getCode());
                stateList.add(GroupStatus.CONFIG_SUCCESSFUL.getCode());
                return stateList;
            case STOPPED:
                stateList.add(GroupStatus.SUSPENDED.getCode());
                return stateList;
            case FINISHED:
                stateList.add(GroupStatus.FINISH.getCode());
                return stateList;
            case DELETED:
                stateList.add(GroupStatus.DELETED.getCode());
                return stateList;
            default:
                throw new IllegalArgumentException(String.format("Unsupported status %s for group", state));
        }
    }
}
