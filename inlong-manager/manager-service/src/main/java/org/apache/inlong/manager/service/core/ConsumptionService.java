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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.manager.common.beans.PageResult;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionInfo;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionListVo;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionQuery;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionSummary;
import org.apache.inlong.manager.common.pojo.dataconsumption.ConsumptionUpdateInfo;
import org.apache.inlong.manager.service.workflow.WorkflowResult;

/**
 * Data consumption interface
 */
public interface ConsumptionService {

    /**
     * Data consumption statistics
     *
     * @param query Query conditions
     * @return Statistics
     */
    ConsumptionSummary getSummary(ConsumptionQuery query);

    /**
     * Get data consumption list according to query conditions
     *
     * @param query
     * @return
     */
    PageResult<ConsumptionListVo> list(ConsumptionQuery query);

    /**
     * Get data consumption details
     *
     * @param id Consumer ID
     * @return Details
     */
    ConsumptionInfo getInfo(Integer id);

    /**
     * According to data consumption details
     *
     * @param consumerGroupId Consumer group ID
     * @return
     */
    ConsumptionInfo getInfo(String consumerGroupId);

    /**
     * Determine whether the Consumer group ID already exists
     *
     * @param consumerGroupId Consumer group ID
     * @param excludeSelfId Exclude the ID of this record
     * @return does it exist
     */
    boolean isConsumerGroupIdExist(String consumerGroupId, Integer excludeSelfId);

    /**
     * Save basic data consumption information
     *
     * @param consumptionInfo Data consumption information
     * @param operator Operator
     * @return ID after saved
     */
    Integer save(ConsumptionInfo consumptionInfo, String operator);

    /**
     * Update the person in charge of data consumption, etc
     *
     * @param consumptionUpdateInfo Update information
     * @param operator Operator
     * @return Updated id
     */
    Integer update(ConsumptionUpdateInfo consumptionUpdateInfo, String operator);

    /**
     * Delete data consumption
     *
     * @param id Consumer ID
     * @param operator Operator
     */
    void delete(Integer id, String operator);

    /**
     * Start the application process
     *
     * @param id Data consumption id
     * @param operator Operator
     * @return Process information
     */
    WorkflowResult startProcess(Integer id, String operator);

}
