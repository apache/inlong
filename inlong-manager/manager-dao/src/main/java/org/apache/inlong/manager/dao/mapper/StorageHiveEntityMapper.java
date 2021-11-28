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

package org.apache.inlong.manager.dao.mapper;

import java.util.List;
import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.common.pojo.datastorage.StorageHiveSortInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageSummaryInfo;
import org.apache.inlong.manager.dao.entity.StorageHiveEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface StorageHiveEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(StorageHiveEntity record);

    int insertSelective(StorageHiveEntity record);

    StorageHiveEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(StorageHiveEntity record);

    int updateByPrimaryKey(StorageHiveEntity record);

    /**
     * Paging query storage list based on conditions
     *
     * @param request Paging query conditions
     * @return Hive storage entity list
     */
    List<StorageHiveEntity> selectByCondition(@Param("request") StoragePageRequest request);

    /**
     * According to the business group id and data stream id, query valid storage information
     *
     * @param groupId business group id
     * @param streamId data stream id
     * @return Hive storage entity list
     */
    List<StorageHiveEntity> selectByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * According to the business group id and data stream id, query the number of valid Hive storage
     *
     * @param groupId business group id
     * @param streamId data stream id
     * @return Hive storage entity size
     */
    int selectCountByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    int updateStorageStatusById(StorageHiveEntity entity);

    /**
     * Given a list of data stream ids, filter out data stream id list with Hive storage
     *
     * @param groupId business group id
     * @param streamIdList data stream id list
     * @return a list of data stream ids with Hive storage
     */
    List<String> selectDataStreamExists(@Param("groupId") String groupId,
            @Param("streamIdList") List<String> streamIdList);

    /**
     * According to the business group id and data stream id, query Hive storage summary information
     */
    List<StorageSummaryInfo> selectSummary(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * Select Hive configs for Sort under the business group id and stream id
     *
     * @param groupId Business group id
     * @param streamId Data stream id, if is null, get all configs under the group id
     * @return Hive Sort config
     */
    List<StorageHiveSortInfo> selectHiveSortInfoByIdentifier(@Param("groupId") String groupId,
            @Param("streamId") String streamId);

}