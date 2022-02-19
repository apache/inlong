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

package org.apache.inlong.manager.workflow.base.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.exceptions.WorkflowNoRollbackException;
import org.apache.inlong.manager.common.exceptions.WorkflowRollbackOnceException;
import org.apache.inlong.manager.dao.mapper.WorkflowProcessEntityMapper;
import org.apache.inlong.manager.dao.mapper.WorkflowTaskEntityMapper;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.base.ProcessorExecutor;
import org.apache.inlong.manager.workflow.base.TransactionHelper;
import org.apache.inlong.manager.workflow.definition.Element;
import org.apache.inlong.manager.workflow.definition.NextableElement;
import org.apache.inlong.manager.workflow.definition.SkippableElement;
import org.apache.inlong.manager.workflow.definition.WorkflowTask;
import org.apache.inlong.manager.workflow.processor.ElementProcessor;
import org.apache.inlong.manager.workflow.processor.EndEventProcessor;
import org.apache.inlong.manager.workflow.processor.ServiceTaskProcessor;
import org.apache.inlong.manager.workflow.processor.SkipableElementProcessor;
import org.apache.inlong.manager.workflow.processor.StartEventProcessor;
import org.apache.inlong.manager.workflow.processor.UserTaskProcessor;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;

import java.util.List;

/**
 * Workload component processor execution
 */
@Slf4j
public class ProcessorExecutorImpl implements ProcessorExecutor {

    private final ImmutableMap<Class<? extends Element>, ElementProcessor<? extends Element>> elementProcessor;

    private final TransactionHelper transactionHelper;

    public ProcessorExecutorImpl(
            WorkflowProcessEntityMapper processEntityMapper,
            WorkflowTaskEntityMapper taskEntityMapper,
            WorkflowEventNotifier workflowEventNotifier,
            TransactionHelper transactionHelper) {

        List<ElementProcessor<? extends Element>> processors = initProcessors(processEntityMapper,
                taskEntityMapper, workflowEventNotifier);
        ImmutableMap.Builder<Class<? extends Element>, ElementProcessor<? extends Element>> builder
                = ImmutableMap.builder();
        processors.forEach(processor -> builder.put(processor.watch(), processor));
        elementProcessor = builder.build();
        this.transactionHelper = transactionHelper;
    }

    private List<ElementProcessor<?>> initProcessors(WorkflowProcessEntityMapper processEntityMapper,
            WorkflowTaskEntityMapper taskEntityMapper, WorkflowEventNotifier eventNotifier) {
        List<ElementProcessor<?>> processors = Lists.newArrayList();
        processors.add(new StartEventProcessor(processEntityMapper, eventNotifier));
        processors.add(new EndEventProcessor(processEntityMapper, taskEntityMapper, eventNotifier));
        processors.add(new UserTaskProcessor(taskEntityMapper, eventNotifier));
        processors.add(new ServiceTaskProcessor(taskEntityMapper, eventNotifier));
        return processors;
    }

    private ElementProcessor<? extends Element> getProcessor(Class<? extends Element> elementClazz) {
        if (!elementProcessor.containsKey(elementClazz)) {
            throw new WorkflowException("element executor not found " + elementClazz.getName());
        }
        return elementProcessor.get(elementClazz);
    }

    @Override
    public void executeStart(Element element, WorkflowContext context) {
        ElementProcessor processor = this.getProcessor(element.getClass());
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
        if (element instanceof WorkflowTask) {
            transactionHelper.execute(executeCompleteInTransaction(element, context),
                    TransactionDefinition.PROPAGATION_NESTED);
            return;
        }

        executeComplete(element, context);
    }

    @Override
    public void executeComplete(Element element, WorkflowContext context) {
        ElementProcessor processor = this.getProcessor(element.getClass());
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

        ElementProcessor processor = this.getProcessor(element.getClass());

        if (!(processor instanceof SkipableElementProcessor)) {
            throw new WorkflowException(
                    "element processor not instance of skip processor " + element.getDisplayName());
        }

        // Execute skip logic
        SkipableElementProcessor skipableProcessor = (SkipableElementProcessor) processor;
        skipableProcessor.skip(element, context);

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
