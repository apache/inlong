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

package org.apache.inlong.manager.service.workflow;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import lombok.Data;
import org.apache.inlong.manager.service.workflow.business.BusinessAdminApproveForm;
import org.apache.inlong.manager.service.workflow.consumption.ConsumptionAdminApproveForm;
import org.apache.inlong.manager.workflow.definition.TaskForm;

/**
 * Approval task node form
 */
@Data
@JsonTypeInfo(use = Id.NAME, property = "formName")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BusinessAdminApproveForm.class, name = BusinessAdminApproveForm.FORM_NAME),
        @JsonSubTypes.Type(value = ConsumptionAdminApproveForm.class, name = ConsumptionAdminApproveForm.FORM_NAME),
})
public abstract class BaseTaskForm implements TaskForm {

}
