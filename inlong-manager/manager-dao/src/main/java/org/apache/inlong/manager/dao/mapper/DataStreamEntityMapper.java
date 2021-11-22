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
import org.apache.inlong.manager.common.pojo.datastream.DataStreamInfoToHiveConfig;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamPageRequest;
import org.apache.inlong.manager.common.pojo.datastream.DataStreamTopicVO;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface DataStreamEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(DataStreamEntity record);

    int insertSelective(DataStreamEntity record);

    DataStreamEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(DataStreamEntity record);

    int updateByPrimaryKey(DataStreamEntity record);

    DataStreamEntity selectByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    Integer selectExistByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * Query all data stream according to conditions (do not specify groupId, query all data streams)
     *
     * @param request query request
     * @return data stream list
     */
    List<DataStreamEntity> selectByCondition(DataStreamPageRequest request);

    List<DataStreamEntity> selectByGroupId(@Param("groupId") String groupId);

    /**
     * According to the conditions and business in charges, query all data stream
     *
     * @param request paging query conditions
     * @param inCharges business in charges
     * @return data stream list
     */
    List<DataStreamEntity> selectByConditionAndInCharges(@Param("request") DataStreamPageRequest request,
            @Param("inCharges") String inCharges);

    List<DataStreamInfoToHiveConfig> selectStreamToHiveInfo(@Param("groupId") String groupId);

    DataStreamInfoToHiveConfig selectStreamToHiveInfoByIdentifier(@Param("groupId") String groupId,
            @Param("streamId") String streamId);

    int selectCountByGroupId(@Param("groupId") String groupId);

    List<DataStreamTopicVO> selectTopicList(@Param("groupId") String groupId);

    /**
     * Physically delete all data streams of the specified business group id
     *
     * @return rows deleted
     */
    int deleteAllByGroupId(@Param("groupId") String groupId);

    int updateByIdentifierSelective(DataStreamEntity streamEntity);

    int updateStatusByIdentifier(@Param("groupId") String groupId, @Param("streamId") String streamId,
            @Param("status") Integer status, @Param("modifier") String modifier);

}