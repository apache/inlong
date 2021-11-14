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
import org.apache.inlong.manager.common.pojo.datastream.DataStreamApproveInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfo;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfoToHiveConfig;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamListVO;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamPageRequest;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamSummaryInfo;
import org.apache.inlong.manager.common.pojo.datastream.FullPageInfo;
import org.apache.inlong.manager.common.pojo.datastream.FullPageUpdateInfo;

/**
 * data stream service layer interface
 *
 * @apiNote It is associated with various DataSources, the upstream is Business, and the downstream is Storage
 */
public interface DataStreamService {

    /**
     * Save data stream information
     *
     * @param dataStreamInfo Basic data stream information
     * @param operator Edit person's name
     * @return Data stream id after successful save
     */
    Integer save(DataStreamInfo dataStreamInfo, String operator);

    /**
     * Query the details of the specified data stream
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return data stream details
     */
    DataStreamInfo get(String groupId, String streamId);

    /**
     * Query data stream list based on conditions
     *
     * @param request Data stream paging query request
     * @return Data stream paging list
     */
    PageInfo<DataStreamListVO> listByCondition(DataStreamPageRequest request);

    /**
     * Query all hive config for business group id
     *
     * @param groupId Business group id
     * @return Hive config list
     */
    List<DataStreamInfoToHiveConfig> queryHiveConfigForAllDataStream(String groupId);

    /**
     * Query hive config for one data stream
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return Hive config
     */
    DataStreamInfoToHiveConfig queryHiveConfigForOneDataStream(String groupId, String streamId);

    /**
     * Business information that needs to be modified
     *
     * @param dataStreamInfo data stream information that needs to be modified
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean update(DataStreamInfo dataStreamInfo, String operator);

    /**
     * Delete the specified data stream
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean delete(String groupId, String streamId, String operator);

    /**
     * Logically delete all data streams under the specified groupId
     *
     * @param groupId Business group id
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean logicDeleteAllByBid(String groupId, String operator);

    /**
     * Obtain the flow of data stream according to groupId
     *
     * @param groupId Business group id
     * @return Summary list of data stream
     */
    List<DataStreamSummaryInfo> getSummaryList(String groupId);

    /**
     * Save all information related to the data stream, its data source, and data storage
     *
     * @param fullPageInfo All information on the page
     * @param operator Edit person's name
     * @return Whether the save was successful
     */
    boolean saveAll(FullPageInfo fullPageInfo, String operator);

    /**
     * Save data streams, their data sources, and all information related to data storage in batches
     *
     * @param fullPageInfoList List of data stream page information
     * @param operator Edit person's name
     * @return Whether the save was successful
     * @apiNote This interface is only used when creating a new business. To ensure data consistency,
     *         all associated data needs to be physically deleted, and then added
     */
    boolean batchSaveAll(List<FullPageInfo> fullPageInfoList, String operator);

    /**
     * Paging query all data of the data stream page under the specified groupId
     *
     * @param request Query
     * @return Paging list of all data on the data stream page
     */
    PageInfo<FullPageInfo> listAllWithGroupId(DataStreamPageRequest request);

    /**
     * According to the business group id, query all data stream information
     *
     * @param groupId Business group id
     * @return Data stream list
     */
    List<DataStreamInfo> listAllByBid(String groupId);

    /**
     * Modify all data streams (including basic information about data sources)
     *
     * @param updateInfo data stream page information
     * @param operator Edit person's name
     * @return Whether the modification is successful
     * @apiNote The data source details and data storage information are modified separately,
     *         not in this all modification interface
     */
    boolean updateAll(FullPageUpdateInfo updateInfo, String operator);

    /**
     * According to the group id, query the number of valid data streams belonging to this service
     *
     * @param groupId Business group id
     * @return Number of data streams
     */
    int selectCountByBid(String groupId);

    /**
     * Save the information modified when the approval is passed
     *
     * @param streamApproveList data stream approval information
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean updateAfterApprove(List<DataStreamApproveInfo> streamApproveList, String operator);

    /**
     * Update stream status
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @param status Modified status
     * @param operator Edit person's name
     * @return whether succeed
     * @apiNote If streamId is null, update all data stream associated with groupId
     */
    boolean updateStatus(String groupId, String streamId, Integer status, String operator);

}
