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

package org.apache.inlong.manager.service.core;

import com.github.pagehelper.PageInfo;
import java.util.List;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageInfo;
import org.apache.inlong.manager.common.pojo.datastorage.BaseStorageListVO;
import org.apache.inlong.manager.common.pojo.datastorage.StorageApproveInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageClusterInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageSummaryInfo;
import org.springframework.web.multipart.MultipartFile;

/**
 * Service layer interface for data storage
 */
public interface StorageService {

    /**
     * Save storage information
     *
     * @param storageInfo Store information
     * @param operator Edit person's name
     * @return Primary key after saving
     */
    Integer save(BaseStorageInfo storageInfo, String operator);

    /**
     * Query storage information based on id
     *
     * @param id Data primary key
     * @param storageType Storage type
     * @return Store information
     */
    BaseStorageInfo getById(String storageType, Integer id);

    /**
     * Query storage information based on business and data stream identifiers
     *
     * @param bid Business identifier
     * @param dsid Data stream identifier，Can be empty
     * @return Store information list
     * @apiNote Storage types only support temporarily: HIVE
     */
    List<BaseStorageInfo> listByIdentifier(String bid, String dsid);

    /**
     * Query stored summary information based on business and data stream identifiers, including storage cluster
     *
     * @param bid Business identifier
     * @param dsid Data stream identifier
     * @return Store information list
     * @apiNote Storage types only support temporarily: HIVE
     */
    List<StorageSummaryInfo> listSummaryByIdentifier(String bid, String dsid);

    /**
     * Query the number of undeleted stored information based on business and data stream identifiers
     *
     * @param bid Business identifier
     * @param dsid Data stream identifier
     * @return Number of stored information
     */
    int getCountByIdentifier(String bid, String dsid);

    /**
     * Paging query storage information based on conditions
     *
     * @param request Paging request
     * @return Store information list
     */
    PageInfo<? extends BaseStorageListVO> listByCondition(StoragePageRequest request);

    /**
     * Modify data storage information
     *
     * @param storageInfo Information that needs to be modified
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean update(BaseStorageInfo storageInfo, String operator);

    /**
     * Delete data storage information based on id
     *
     * @param storageType Storage type
     * @param id The primary key of the data store
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean delete(String storageType, Integer id, String operator);

    /**
     * Modify storage data status
     *
     * @param id Stored id
     * @param status Goal state
     * @param log Modify the description
     */
    void updateHiveStatusById(int id, int status, String log);

    /**
     * Query the storage cluster of the specified storage type/area ID
     *
     * @param storageType Storage type
     * @return Store cluster information
     */
    StorageClusterInfo listStorageCluster(String storageType);

    /**
     * Physically delete data storage information under specified conditions
     *
     * @param bid Business identifier
     * @param dsid Data stream identifier
     * @return whether succeed
     */
    boolean deleteAllByIdentifier(String bid, String dsid);

    /**
     * Tombstone data storage information
     *
     * @param businessIdentifier The business identifier to which the data source belongs
     * @param dataStreamIdentifier The data stream identifier to which the data source belongs
     * @param operator Operator name
     * @return whether succeed
     */
    boolean logicDeleteAllByIdentifier(String businessIdentifier, String dataStreamIdentifier, String operator);

    /**
     * According to the existing data stream ID list, filter out the data stream ID list containing the specified
     * storage type
     *
     * @param bid Business identifier
     * @param storageType Storage type
     * @param dataStreamIdentifierList Data stream ID list
     * @return List of filtered data stream IDs
     */
    List<String> filterStreamIdByStorageType(String bid, String storageType, List<String> dataStreamIdentifierList);

    /**
     * According to the data stream identifier, query the list of storage types owned by it
     *
     * @param bid Business identifier
     * @param dsid Data stream identifier
     * @return List of storage types
     */
    List<String> getStorageTypeList(String bid, String dsid);

    /**
     * Save the information modified when the approval is passed
     *
     * @param storageApproveList Data storage approval information
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean updateAfterApprove(List<StorageApproveInfo> storageApproveList, String operator);

}
