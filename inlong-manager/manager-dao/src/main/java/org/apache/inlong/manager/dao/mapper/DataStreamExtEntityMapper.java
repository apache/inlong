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
import org.apache.inlong.manager.dao.entity.DataStreamExtEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface DataStreamExtEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(DataStreamExtEntity record);

    int insertSelective(DataStreamExtEntity record);

    DataStreamExtEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(DataStreamExtEntity record);

    int updateByPrimaryKey(DataStreamExtEntity record);

    List<DataStreamExtEntity> selectByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

    /**
     * Query the undeleted extended attributes based on the business identifier, data stream identifier, and keyName
     *
     * @param keyName attribute name
     * @return extended attribute
     */
    DataStreamExtEntity selectByIdentifierAndKeyName(@Param("bid") String bid, @Param("dsid") String dsid,
            @Param("keyName") String keyName);

    /**
     * Insert data in batches, update if it exists, create new if it does not exist
     */
    int insertAll(@Param("extList") List<DataStreamExtEntity> extEntityList);

    /**
     * According to the business identifier and data stream identifier, physically delete all extended fields
     */
    int deleteAllByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

    /**
     * According to the business identifier and data stream identifier, logically delete all extended fields
     */
    int logicDeleteAllByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

}