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
import org.apache.inlong.manager.dao.entity.StorageHiveFieldEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface StorageHiveFieldEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(StorageHiveFieldEntity record);

    int insertSelective(StorageHiveFieldEntity record);

    StorageHiveFieldEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(StorageHiveFieldEntity record);

    int updateByPrimaryKey(StorageHiveFieldEntity record);

    List<StorageHiveFieldEntity> selectHiveFields(@Param("groupId") String groupId, @Param("streamId") String streamId);

    /**
     * According to the storage primary key, logically delete the corresponding field information
     *
     * @param storageId storage id
     * @return rows deleted
     */
    int logicDeleteAll(@Param("storageId") Integer storageId);

    /**
     * According to the storage primary key, physically delete the corresponding field information
     *
     * @param storageId storage id
     * @return rows deleted
     */
    int deleteAllByStorageId(@Param("storageId") Integer storageId);

    /**
     * According to the storage id, query the Hive field
     *
     * @param storageId storage id
     * @return Hive field list
     */
    List<StorageHiveFieldEntity> selectByStorageId(@Param("storageId") Integer storageId);

}