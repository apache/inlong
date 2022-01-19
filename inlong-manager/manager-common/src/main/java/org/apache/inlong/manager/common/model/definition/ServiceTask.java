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

package org.apache.inlong.manager.common.model.definition;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.WorkflowContext;

/**
 * System task
 */
public class ServiceTask extends Task {

    private static final Set<Action> SUPPORTED_ACTIONS = ImmutableSet
            .of(Action.COMPLETE, Action.CANCEL, Action.TERMINATE);

    @Override
    public Action defaultNextAction() {
        return Action.COMPLETE;
    }

    @Override
    protected Set<Action> supportedActions() {
        return SUPPORTED_ACTIONS;
    }

    @Override
    public List<Element> getNextList(Action action, WorkflowContext context) {
        Preconditions.checkTrue(supportedActions().contains(action),
                () -> "not support action " + action + " ,action should in one of " + supportedActions());

        switch (action) {
            case COMPLETE:
                return super.getNextList(action, context);
            case CANCEL:
            case TERMINATE:
                return Collections.singletonList(context.getProcess().getEndEvent());
            default:
                throw new WorkflowException("unknown action " + action);
        }
    }
}
