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

package org.apache.inlong.manager.workflow.core.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.workflow.TransactionHelper;
import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.workflow.WorkflowProcessorExecutor;
import org.apache.inlong.manager.workflow.core.processor.EndEventProcessor;
import org.apache.inlong.manager.workflow.core.processor.ServiceTaskProcessor;
import org.apache.inlong.manager.workflow.core.processor.SkipAbleElementProcessor;
import org.apache.inlong.manager.workflow.core.processor.StartEventProcessor;
import org.apache.inlong.manager.workflow.core.processor.UserTaskProcessor;
import org.apache.inlong.manager.workflow.core.processor.WorkflowElementProcessor;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.exceptions.WorkflowNoRollbackException;
import org.apache.inlong.manager.common.exceptions.WorkflowRollbackOnceException;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Element;
import org.apache.inlong.manager.common.model.definition.NextableElement;
import org.apache.inlong.manager.common.model.definition.SkippableElement;
import org.apache.inlong.manager.common.model.definition.Task;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;

/**
 * Workload component processor execution
 */
@Slf4j
public class WorkflowProcessorExecutorImpl implements WorkflowProcessorExecutor {

    private final ImmutableMap<Class<? extends Element>, WorkflowElementProcessor> workflowElementProcessor;

    private TransactionHelper transactionHelper;

    public WorkflowProcessorExecutorImpl(WorkflowDataAccessor workflowDataAccessor,
            WorkflowEventNotifier workflowEventNotifier, TransactionHelper transactionHelper) {

        List<WorkflowElementProcessor<? extends Element>> processors = initProcessors(workflowDataAccessor,
                workflowEventNotifier);
        ImmutableMap.Builder<Class<? extends Element>, WorkflowElementProcessor> builder = ImmutableMap.builder();
        processors.forEach(processor -> builder.put(processor.watch(), processor));
        workflowElementProcessor = builder.build();
        this.transactionHelper = transactionHelper;
    }

    private List<WorkflowElementProcessor<?>> initProcessors(WorkflowDataAccessor workflowDataAccessor,
            WorkflowEventNotifier workflowEventNotifier) {
        List<WorkflowElementProcessor<?>> processors = Lists.newArrayList();
        processors.add(new StartEventProcessor(workflowDataAccessor, workflowEventNotifier));
        processors.add(new EndEventProcessor(workflowDataAccessor, workflowEventNotifier));
        processors.add(new UserTaskProcessor(workflowDataAccessor, workflowEventNotifier));
        processors.add(new ServiceTaskProcessor(workflowDataAccessor, workflowEventNotifier));
        return processors;
    }

    private WorkflowElementProcessor getProcessor(Class<? extends Element> elementClazz) {
        if (!workflowElementProcessor.containsKey(elementClazz)) {
            throw new WorkflowException("element executor not found " + elementClazz.getName());
        }
        return workflowElementProcessor.get(elementClazz);
    }

    @Override
    public void executeStart(Element element, WorkflowContext context) {
        WorkflowElementProcessor processor = this.getProcessor(element.getClass());
        context.setCurrentElement(element);

        // If the current component needs to be skipped, proceed directly to the next one
        if (isSkipCurrentElement(element, context)) {
            executeSkipAndNext(element, context);
            return;
        }

        processor.create(element, context);
        if (processor.pendingForAction(context)) {
            return;
        }

        // If it is a continuous task execution transaction isolation
        if (element instanceof Task) {
            transactionHelper
                    .execute(executeCompleteInTransaction(element, context), TransactionDefinition.PROPAGATION_NESTED);
            return;
        }

        executeComplete(element, context);
    }

    @Override
    public void executeComplete(Element element, WorkflowContext context) {
        WorkflowElementProcessor processor = this.getProcessor(element.getClass());
        context.setCurrentElement(element);
        boolean completed = processor.complete(context);
        if (!completed) {
            return;
        }
        List<Element> nextElements = processor.next(element, context);
        nextElements.forEach(next -> executeStart(next, context));
    }

    private boolean isSkipCurrentElement(Element element, WorkflowContext context) {
        return (element instanceof SkippableElement) && ((SkippableElement) element).isSkip(context);
    }

    private void executeSkipAndNext(Element element, WorkflowContext context) {
        if (!(element instanceof SkippableElement)) {
            throw new WorkflowException("element not instance of skip element " + element.getDisplayName());
        }

        if (!(element instanceof NextableElement)) {
            throw new WorkflowException("element not instance of nextable element " + element.getDisplayName());
        }

        WorkflowElementProcessor processor = this.getProcessor(element.getClass());

        if (!(processor instanceof SkipAbleElementProcessor)) {
            throw new WorkflowException(
                    "element processor not instance of skip processor " + element.getDisplayName());
        }

        // Execute skip logic
        ((SkipAbleElementProcessor) processor).skip(element, context);

        // Execute next
        context.getActionContext().setAction(((NextableElement) element).defaultNextAction());
        List<Element> nextElements = processor.next(element, context);
        nextElements.forEach(next -> executeStart(next, context));
    }

    private TransactionCallback<Object> executeCompleteInTransaction(Element element, WorkflowContext context) {
        return s -> {
            try {
                executeComplete(element, context);
                return null;
            } catch (WorkflowNoRollbackException e) { // Exception does not roll back
                throw e;
            } catch (Exception e) { // The exception is only rolled back once
                throw new WorkflowRollbackOnceException(e.getMessage());
            }
        };
    }

}
