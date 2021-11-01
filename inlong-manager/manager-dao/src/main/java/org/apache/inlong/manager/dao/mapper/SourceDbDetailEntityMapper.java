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
import org.apache.inlong.manager.common.pojo.datasource.SourceDbDetailPageRequest;
import org.apache.inlong.manager.dao.entity.SourceDbDetailEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface SourceDbDetailEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(SourceDbDetailEntity record);

    int insertSelective(SourceDbDetailEntity record);

    SourceDbDetailEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(SourceDbDetailEntity record);

    int updateByPrimaryKey(SourceDbDetailEntity record);

    List<SourceDbDetailEntity> selectByCondition(SourceDbDetailPageRequest request);

    Integer selectDetailExist(String groupId, String streamId, String dbName, String connectionName);

    /**
     * According to the business group id and data stream identifier, query data source details
     */
    List<SourceDbDetailEntity> selectByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * According to the business group id and data stream identifier, physically delete DB data source details
     */
    int deleteByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * According to the business group id and data stream identifier, logically delete DB data source details
     */
    int logicDeleteByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("operator") String operator);

}