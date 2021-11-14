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
import org.apache.inlong.manager.dao.entity.BusinessExtEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface BusinessExtEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(BusinessExtEntity record);

    int insertSelective(BusinessExtEntity record);

    BusinessExtEntity selectByPrimaryKey(Integer id);

    List<BusinessExtEntity> selectByGroupId(String groupId);

    int updateByPrimaryKeySelective(BusinessExtEntity record);

    int updateByPrimaryKey(BusinessExtEntity record);

    BusinessExtEntity selectByGroupIdAndKeyName(String groupId, String keyName);

    /**
     * Insert data in batches, update if it exists, create new if it does not exist
     *
     * @param extEntityList need to insert data
     */
    int insertAll(@Param("extList") List<BusinessExtEntity> extEntityList);

    /**
     * Physically delete all extension fields based on the business group id
     *
     * @return rows deleted
     */
    int deleteAllByGroupId(String groupId);

    /**
     * Logically delete all extended fields based on business group id
     *
     * @return rows updated
     */
    int logicDeleteAllByGroupId(String groupId);

}