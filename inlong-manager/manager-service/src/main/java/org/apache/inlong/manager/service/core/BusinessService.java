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

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessApproveInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessCountVO;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessListVO;
import org.apache.inlong.manager.common.pojo.business.BusinessPageRequest;
import org.apache.inlong.manager.common.pojo.business.BusinessTopicVO;

/**
 * Business access service layer interface
 */
public interface BusinessService {

    /**
     * Save business information
     *
     * @param businessInfo Basic business information
     * @param operator Operator name
     * @return Business group id after successfully saved
     */
    String save(BusinessInfo businessInfo, String operator);

    /**
     * Query business information based on group id
     *
     * @param groupId Business group id
     * @return Business details
     */
    BusinessInfo get(String groupId);

    /**
     * Query business list based on conditions
     *
     * @param request Business pagination query request
     * @return Business Pagination List
     */
    PageInfo<BusinessListVO> listByCondition(BusinessPageRequest request);

    /**
     * Modify business information
     *
     * @param businessInfo Business information that needs to be modified
     * @param operator Operator name
     * @return Business group id
     */
    String update(BusinessInfo businessInfo, String operator);

    /**
     * Modify the status of the specified business
     *
     * @param groupId Business group id
     * @param status Modified status
     * @param operator Current operator
     * @return whether succeed
     */
    boolean updateStatus(String groupId, Integer status, String operator);

    /**
     * Delete the business information of the specified group id
     *
     * @param groupId The business group id that needs to be deleted
     * @param operator Current operator
     * @return whether succeed
     */
    boolean delete(String groupId, String operator);

    /**
     * Query whether the specified group id exists
     *
     * @param groupId The business group id to be queried
     * @return does it exist
     */
    boolean exist(String groupId);

    /**
     * Query the business information of each status of the current user
     *
     * @param operator Current operator
     * @return Business status statistics
     */
    BusinessCountVO countBusinessByUser(String operator);

    /**
     * According to the business group id, query the topic to which it belongs
     *
     * @param groupId Business group id
     * @return Topic information
     * @apiNote Tube corresponds to the business, only 1 topic
     */
    BusinessTopicVO getTopic(String groupId);

    /**
     * Save the business modified when the approval is passed
     *
     * @param approveInfo Approval information
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean updateAfterApprove(BusinessApproveInfo approveInfo, String operator);

}
