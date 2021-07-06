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

import org.apache.inlong.manager.common.pojo.commonserver.CommonFileServerPageRequest;
import org.apache.inlong.manager.dao.entity.CommonFileServerEntity;

import java.util.List;

import org.springframework.stereotype.Repository;

@Repository
public interface CommonFileServerEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(CommonFileServerEntity record);

    int insertSelective(CommonFileServerEntity record);

    CommonFileServerEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(CommonFileServerEntity record);

    int updateByPrimaryKey(CommonFileServerEntity record);

    List<CommonFileServerEntity> selectByUsernameAndIpPort(String username, String ip, int port);

    List<CommonFileServerEntity> selectAll();

    List<CommonFileServerEntity> selectByCondition(CommonFileServerPageRequest request);
}