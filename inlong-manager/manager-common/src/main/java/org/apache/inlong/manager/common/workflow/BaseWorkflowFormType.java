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

package org.apache.inlong.manager.common.workflow;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import lombok.Data;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.common.workflow.bussiness.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.common.workflow.bussiness.NewBusinessWorkflowForm;
import org.apache.inlong.manager.common.workflow.bussiness.UpdateBusinessWorkflowForm;
import org.apache.inlong.manager.common.workflow.consumption.NewConsumptionWorkflowForm;

/**
 * The main form of the process-submitted when the process is initiated
 *
 */
@Data
@JsonTypeInfo(use = Id.NAME, property = "formName")
@JsonSubTypes({
        @JsonSubTypes.Type(value = NewBusinessWorkflowForm.class, name = NewBusinessWorkflowForm.FORM_NAME),
        @JsonSubTypes.Type(value = NewConsumptionWorkflowForm.class, name = NewConsumptionWorkflowForm.FORM_NAME),
        @JsonSubTypes.Type(value = BusinessResourceWorkflowForm.class, name = BusinessResourceWorkflowForm.FORM_NAME),
        @JsonSubTypes.Type(value = UpdateBusinessWorkflowForm.class, name = UpdateBusinessWorkflowForm.FORM_NAME)
})
public abstract class BaseWorkflowFormType implements ProcessForm {

}
