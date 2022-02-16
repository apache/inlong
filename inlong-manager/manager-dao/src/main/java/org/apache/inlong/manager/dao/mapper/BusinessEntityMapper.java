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
import org.apache.inlong.commons.pojo.dataproxy.DataProxyConfig;
import org.apache.inlong.manager.common.pojo.business.BusinessPageRequest;
import org.apache.inlong.manager.dao.entity.BusinessEntity;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface BusinessEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(BusinessEntity record);

    int insertSelective(BusinessEntity record);

    BusinessEntity selectByPrimaryKey(Integer id);

    List<Map<String, Object>> countCurrentUserBusiness(@Param(value = "currentUser") String currentUser);

    BusinessEntity selectByIdentifier(String groupId);

    Integer selectIdentifierExist(String groupId);

    List<BusinessEntity> selectByCondition(BusinessPageRequest request);

    List<BusinessEntity> selectAll(Integer status);

    /**
     * get all config with business status of 130, that is, config successful
     */
    List<DataProxyConfig> selectDataProxyConfig();

    int updateByPrimaryKeySelective(BusinessEntity record);

    int updateByIdentifierSelective(BusinessEntity record);

    int updateByPrimaryKey(BusinessEntity record);

    int updateStatusByIdentifier(@Param("groupId") String groupId, @Param("status") Integer status,
                                 @Param("modifier") String modifier);

    List<String> selectGroupIdByProxyId(Integer proxyClusterId);

}