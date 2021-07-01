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
import org.apache.inlong.manager.dao.entity.StorageExtEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface StorageExtEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(StorageExtEntity record);

    int insertSelective(StorageExtEntity record);

    StorageExtEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(StorageExtEntity record);

    int updateByPrimaryKey(StorageExtEntity record);

    /**
     * According to the storage type and storage id, physically delete the corresponding extended information
     *
     * @param storageType storage type
     * @param storageId storage id
     * @return rows deleted
     */
    int deleteByStorageTypeAndId(@Param("storageType") String storageType, @Param("storageId") Integer storageId);

    /**
     * According to the storage type and storage id, logically delete the corresponding extended information
     *
     * @param storageId storage id
     * @return rows updated
     */
    int logicDeleteAll(@Param("storageId") Integer storageId);

    /**
     * According to the storage type and storage primary key, query the corresponding extended information
     *
     * @param storageType storage type
     * @param storageId storage id
     * @return extended info list
     */
    List<StorageExtEntity> selectByStorageTypeAndId(@Param("storageType") String storageType,
            @Param("storageId") Integer storageId);
}