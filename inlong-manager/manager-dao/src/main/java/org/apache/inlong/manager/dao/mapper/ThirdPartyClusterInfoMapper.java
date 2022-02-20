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
import org.apache.inlong.manager.common.pojo.cluster.ClusterRequest;
import org.apache.inlong.manager.dao.entity.ThirdPartyClusterInfoEntity;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ThirdPartyClusterInfoMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(ThirdPartyClusterInfoEntity record);

    int insertSelective(ThirdPartyClusterInfoEntity record);

    ThirdPartyClusterInfoEntity selectByPrimaryKey(Integer id);

    List<ThirdPartyClusterInfoEntity> selectByIdList(@Param("idList") List<Integer> idList);

    int updateByPrimaryKeySelective(ThirdPartyClusterInfoEntity record);

    int updateByPrimaryKey(ThirdPartyClusterInfoEntity record);

    List<ThirdPartyClusterInfoEntity> selectByCondition(ClusterRequest request);

}