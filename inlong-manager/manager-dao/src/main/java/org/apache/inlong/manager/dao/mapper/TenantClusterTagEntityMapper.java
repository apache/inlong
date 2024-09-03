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

import org.apache.inlong.manager.dao.entity.TenantClusterTagEntity;
import org.apache.inlong.manager.pojo.cluster.TenantClusterTagPageRequest;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TenantClusterTagEntityMapper {

    int updateByIdSelective(TenantClusterTagEntity record);

    int insert(TenantClusterTagEntity record);

    TenantClusterTagEntity selectByPrimaryKey(Integer id);

    TenantClusterTagEntity selectByUniqueKey(@Param("clusterTag") String clusterTag, @Param("tenant") String tenant);

    List<TenantClusterTagEntity> selectByTag(String clusterTag);

    List<TenantClusterTagEntity> selectByCondition(TenantClusterTagPageRequest request);

    int copy(@Param("clusterTag") String clusterTag, @Param("sourceTenant") String sourceTenant,
            @Param("targetTenant") String targetTenant);

}