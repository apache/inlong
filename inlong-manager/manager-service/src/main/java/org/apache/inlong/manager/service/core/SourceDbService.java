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
import org.apache.inlong.manager.common.pojo.datasource.SourceDbBasicInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailInfo;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailListVO;
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailPageRequest;

/**
 * DB data source service layer interface
 *
 */
public interface SourceDbService {

    /**
     * Save the basic information of the DB data source
     *
     * @param basicInfo DB data source basic information
     * @param operator perator name
     * @return Primary key after saving
     */
    Integer saveBasic(SourceDbBasicInfo basicInfo, String operator);

    /**
     * Query the basic information of the data source based on the business group id and the data stream identifier
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return Basic data source information
     */
    SourceDbBasicInfo getBasicByIdentifier(String groupId, String streamId);

    /**
     * Modify the basic information of the data source
     *
     * @param basicInfo Data source information that needs to be modified
     * @param operator perator name
     * @return whether succeed
     */
    boolean updateBasic(SourceDbBasicInfo basicInfo, String operator);

    /**
     * Tombstone data source basic information
     *
     * @param id Data source basic information id
     * @return whether succeed
     */
    boolean logicDeleteBasic(Integer id, String operator);

    /**
     * ave DB data source details
     *
     * @param detailInfo DB data source details
     * @param operator perator name
     * @return Primary key after saving
     */
    Integer saveDetail(SourceDbDetailInfo detailInfo, String operator);

    /**
     * Query DB data source details based on id
     *
     * @param id Data source id
     * @return Data source details
     */
    SourceDbDetailInfo getDetailById(Integer id);

    /**
     * Query a detailed list of DB data sources based on business and data stream identifiers
     *
     * @param groupId Business group id
     * @param streamId Data stream id, can be null
     * @return Data source details
     */
    List<SourceDbDetailInfo> listDetailByIdentifier(String groupId, String streamId);

    /**
     * Query the detailed list of data sources based on conditions
     *
     * @param request Data source paging query request
     * @return Data source pagination list
     */
    PageInfo<SourceDbDetailListVO> listByCondition(SourceDbDetailPageRequest request);

    /**
     * Modify data source details
     *
     * @param detailInfo Data source information that needs to be modified
     * @param operator perator name
     * @return whether succeed
     */
    boolean updateDetail(SourceDbDetailInfo detailInfo, String operator);

    /**
     * Tombstone data source details
     *
     * @param id Data source id
     * @param operator perator name
     * @return whether succeed
     */
    boolean logicDeleteDetail(Integer id, String operator);

    /**
     * Physically delete the basic and detailed information of the data source
     *
     * @param groupId The business group id to which the data source belongs
     * @param streamId The data stream identifier to which the data source belongs
     * @return whether succeed
     */
    boolean deleteAllByIdentifier(String groupId, String streamId);

    /**
     * Tombstone data source basic information and detailed information
     *
     * @param groupId The business group id to which the data source belongs
     * @param streamId The data stream identifier to which the data source belongs
     * @param operator perator name
     * @return whether succeed
     */
    boolean logicDeleteAllByIdentifier(String groupId, String streamId, String operator);

}