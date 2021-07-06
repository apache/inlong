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
    List<StorageHiveEntity> selectByCondition(StoragePageRequest request);

    /**
     * According to the business identifier and data stream identifier, query valid storage information
     *
     * @param bid business identifier
     * @param dsid data stream identifier
     * @return Hive storage entity list
     */
    List<StorageHiveEntity> selectByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

    /**
     * According to the business identifier and data stream identifier, query the number of valid Hive storage
     *
     * @param bid business identifier
     * @param dsid data stream identifier
     * @return Hive storage entity size
     */
    int selectCountByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

    int updateStorageStatusById(StorageHiveEntity entity);

    /**
     * Given a list of data stream ids, filter out data stream id list with Hive storage
     *
     * @param bid business identifier
     * @param dsidList data stream identifier list
     * @return a list of data stream ids with Hive storage
     */
    List<String> selectDataStreamExists(@Param("bid") String bid, @Param("dsidList") List<String> dsidList);

    /**
     * According to the business identifier and data stream identifier, query Hive storage summary information
     */
    List<StorageSummaryInfo> selectSummaryByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

}