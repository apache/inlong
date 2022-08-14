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

package org.apache.inlong.manager.service.consume;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.pojo.consumption.ConsumptionListVo;
import org.apache.inlong.manager.pojo.consumption.ConsumptionQuery;
import org.apache.inlong.manager.pojo.consumption.ConsumptionSummary;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeRequest;

/**
 * Inlong consume service layer interface
 */
public interface InlongConsumeService {

    Integer save(InlongConsumeRequest consumeRequest, String operator);

    /**
     * Determine whether the Consumer group already exists
     *
     * @param consumerGroup Consumer group
     * @param excludeSelfId Exclude the ID of this record
     * @return does it exist
     */
    boolean isConsumerGroupExists(String consumerGroup, Integer excludeSelfId);

    /**
     * Update the person in charge of data consumption, etc
     *
     * @param consumeRequest consume request
     * @param operator operator
     */
    Boolean update(InlongConsumeRequest consumeRequest, String operator);

    InlongConsumeInfo get(Integer id);

    Boolean delete(Integer id, String operator);

    /**
     * Get data consumption list according to query conditions
     *
     * @param query Consumption info
     * @return Consumption list
     */
    PageInfo<ConsumptionListVo> list(ConsumptionQuery query);

    /**
     * Data consumption statistics
     *
     * @param query Query conditions
     * @return Statistics
     */
    ConsumptionSummary getSummary(ConsumptionQuery query);
}
