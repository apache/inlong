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
import org.apache.inlong.manager.common.pojo.source.SourceFileDetailPageRequest;
import org.apache.inlong.manager.dao.entity.SourceFileDetailEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SourceFileDetailEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(SourceFileDetailEntity record);

    int insertSelective(SourceFileDetailEntity record);

    SourceFileDetailEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(SourceFileDetailEntity record);

    int updateByPrimaryKey(SourceFileDetailEntity record);

    /**
     * Update source db detail status after approving inlong group
     *
     * @param groupId Inlong group id
     * @param streamId Inlong stream id
     * @param status Modified status
     * @param modifier Modifier name
     * @return whether succeed
     * @apiNote If stream id is null, update all inlong stream associated with group id
     */
    boolean updateStatusAfterApprove(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("status") Integer status, @Param("modifier") String modifier);

    List<SourceFileDetailEntity> selectByCondition(SourceFileDetailPageRequest request);

    /**
     * Query whether the same file data source details exist
     *
     * @param groupId inlong group id
     * @param streamId inlong stream id
     * @param ip IP of file source
     * @param username username corresponding to the data source IP
     * @return number of eligible file sources
     */
    Integer selectDetailExist(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("ip") String ip, @Param("username") String username);

    /**
     * According to inlong group id and inlong stream id, query file source details
     *
     * @param groupId inlong group id
     * @param streamId inlong stream id
     * @return file source list
     */
    List<SourceFileDetailEntity> selectByIdentifier(@Param("groupId") String groupId,
            @Param("streamId") String streamId);

    /**
     * According to inlong group id and inlong stream id, physically delete file data source details
     *
     * @return rows deleted
     */
    int deleteByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * According to inlong group id and inlong stream id, logically delete file data source details
     *
     * @return rows updated
     */
    int logicDeleteByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("operator") String operator);

}