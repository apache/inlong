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

package org.apache.inlong.manager.workflow.model.definition;

import com.google.common.collect.ImmutableSet;

import org.apache.inlong.manager.workflow.model.Action;
import org.apache.inlong.manager.workflow.model.WorkflowContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Start event
 */
public class StartEvent extends NextableElement {

    private static final Set<Action> SUPPORTED_ACTIONS = ImmutableSet.of(Action.START);

    public StartEvent() {
        this.setName("StartEvent");
        this.setDisplayName("StartEvent");
    }

    @Override
    public List<Element> getNextList(Action action, WorkflowContext context) {
        return getActionToNextElementMap().getOrDefault(action, NextableElement.EMPTY_NEXT)
                .stream()
                .filter(conditionNextElement -> conditionNextElement.getCondition().test(context))
                .map(ConditionNextElement::getElement)
                .collect(Collectors.toList());
    }

    @Override
    public Action defaultNextAction() {
        return Action.START;
    }

    @Override
    protected Set<Action> supportedActions() {
        return SUPPORTED_ACTIONS;
    }
}
