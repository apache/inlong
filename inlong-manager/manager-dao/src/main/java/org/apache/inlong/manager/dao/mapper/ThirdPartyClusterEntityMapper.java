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
import org.apache.inlong.manager.common.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.dao.entity.ThirdPartyClusterEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ThirdPartyClusterEntityMapper {

    int insert(ThirdPartyClusterEntity record);

    int insertSelective(ThirdPartyClusterEntity record);

    ThirdPartyClusterEntity selectByPrimaryKey(Integer id);

    List<ThirdPartyClusterEntity> selectByCondition(ClusterPageRequest request);

    List<ThirdPartyClusterEntity> selectByIdList(@Param("idList") List<Integer> idList);

    List<ThirdPartyClusterEntity> selectByType(@Param("type") String type);

    List<ThirdPartyClusterEntity> selectMQCluster(@Param("mqSetName") String mqSetName,
            @Param("typeList") List<String> typeList);

    ThirdPartyClusterEntity selectByName(@Param("name") String name);

    int updateByPrimaryKeySelective(ThirdPartyClusterEntity record);

    int updateByPrimaryKey(ThirdPartyClusterEntity record);

    int deleteByPrimaryKey(Integer id);

}