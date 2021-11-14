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
import java.util.List;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailListVO;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailPageRequest;

/**
 * File data source service layer interface
 */
public interface SourceFileService {

    /**
     * Save the basic information of the file data source
     *
     * @param basicInfo Basic information of file data source
     * @param operator Operator name
     * @return Primary key after saving
     */
    Integer saveBasic(SourceFileBasicInfo basicInfo, String operator);

    /**
     * Query the basic information of the data source based on the data stream id
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return Basic data source information
     */
    SourceFileBasicInfo getBasicByIdentifier(String groupId, String streamId);

    /**
     * Modify the basic information of the data source
     *
     * @param basicInfo Data source information that needs to be modified
     * @param operator Operator name
     * @return Whether succeed
     */
    boolean updateBasic(SourceFileBasicInfo basicInfo, String operator);

    /**
     * Tombstone data source basic information
     *
     * @param id Data source basic information id
     * @param operator Operator name
     * @return Whether succeed
     */
    boolean logicDeleteBasic(Integer id, String operator);

    /**
     * Save file data source details
     *
     * @param detailInfo File data source details
     * @param operator Operator name
     * @return Primary key after saving
     */
    Integer saveDetail(SourceFileDetailInfo detailInfo, String operator);

    /**
     * Query file data source details based on id
     *
     * @param id Data source id
     * @return Data source details
     */
    SourceFileDetailInfo getDetailById(Integer id);

    /**
     * Query a detailed list of file data sources based on business group id and data stream id
     *
     * @param groupId Business group id
     * @param streamId Data stream id, can be null
     * @return Data source details
     */
    List<SourceFileDetailInfo> listDetailByIdentifier(String groupId, String streamId);

    /**
     * Query the detailed list of data sources based on conditions
     *
     * @param request Data source paging query request
     * @return Data source pagination list
     */
    PageInfo<SourceFileDetailListVO> listByCondition(SourceFileDetailPageRequest request);

    /**
     * Modify data source details
     *
     * @param detailInfo Data source information that needs to be modified
     * @param operator Operator name
     * @return Whether succeed
     */
    boolean updateDetail(SourceFileDetailInfo detailInfo, String operator);

    /**
     * Tombstone data source details
     *
     * @param id Data source id
     * @param operator Operator name
     * @return Whether succeed
     */
    boolean logicDeleteDetail(Integer id, String operator);

    /**
     * Physically delete the basic and detailed information of the data source
     *
     * @param groupId The business group id to which the data source belongs
     * @param streamId The data stream id to which the data source belongs
     * @return Whether succeed
     */
    boolean deleteAllByIdentifier(String groupId, String streamId);

    /**
     * Tombstone data source basic information and detailed information
     *
     * @param groupId The business group id to which the data source belongs
     * @param streamId The data stream id to which the data source belongs
     * @param operator Operator name
     * @return Whether succeed
     */
    boolean logicDeleteAllByIdentifier(String groupId, String streamId, String operator);

}