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

import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.common.pojo.datastorage.StorageBriefResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageForSortDTO;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.dao.entity.StorageEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StorageEntityMapper {

    int insert(StorageEntity record);

    int insertSelective(StorageEntity record);

    StorageEntity selectByPrimaryKey(Integer id);

    /**
     * According to the business group id and data stream id, query the number of valid Hive storage
     *
     * @param groupId business group id
     * @param streamId data stream id
     * @return Hive storage entity size
     */
    int selectCount(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * Paging query storage list based on conditions
     *
     * @param request Paging query conditions
     * @return Hive storage entity list
     */
    List<StorageEntity> selectByCondition(@Param("request") StoragePageRequest request);

    /**
     * Query the storage summary from the given groupId and streamId
     */
    List<StorageBriefResponse> selectSummary(@Param("groupId") String groupId,
            @Param("streamId") String streamId);

    /**
     * According to the business group id and data stream id, query valid storage information
     *
     * @param groupId business group id
     * @param streamId data stream id
     * @return Hive storage entity list
     */
    List<StorageEntity> selectByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * According to the group id, stream id and storage type, query valid storage entity list.
     *
     * @param groupId business group id.
     * @param streamId data stream id.
     * @param storageType storage type.
     * @return storage entity list.
     */
    List<StorageEntity> selectByIdAndType(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("storageType") String storageType);

    /**
     * Filter stream ids with the specified groupId and storageType from the given stream id list.
     *
     * @param groupId InLong group id.
     * @param storageType Storage type.
     * @param streamIdList InLong stream id list.
     * @return List of InLong stream id with the given storage type
     */
    List<String> selectExistsStreamId(@Param("groupId") String groupId, @Param("storageType") String storageType,
            @Param("streamIdList") List<String> streamIdList);

    /**
     * Get the distinct storage type from the given groupId and streamId
     */
    List<String> selectStorageType(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * Select all config for Sort under the group id and stream id.
     *
     * @param groupId Data group id.
     * @param streamId Data stream id, if is null, get all configs under the group id.
     * @return Sort config.
     */
    List<StorageForSortDTO> selectAllConfig(@Param("groupId") String groupId, @Param("streamId") String streamId);

    int updateByPrimaryKeySelective(StorageEntity record);

    int updateByPrimaryKey(StorageEntity record);

    int updateStorageStatus(StorageEntity entity);

    int deleteByPrimaryKey(Integer id);

}