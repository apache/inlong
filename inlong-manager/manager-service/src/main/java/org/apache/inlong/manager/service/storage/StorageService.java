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

package org.apache.inlong.manager.service.storage;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.datastorage.StorageApproveDTO;
import org.apache.inlong.manager.common.pojo.datastorage.StorageBriefResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StoragePageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;

import java.util.List;

/**
 * Service layer interface for data storage
 */
public interface StorageService {

    /**
     * Save the storage information
     *
     * @param request Storage request.
     * @param operator Operator's name.
     * @return Storage id after saving.
     */
    Integer save(StorageRequest request, String operator);

    /**
     * Query storage information based on id and type.
     *
     * @param id Storage id.
     * @param storageType Storage type.
     * @return Store info
     */
    StorageResponse get(Integer id, String storageType);

    /**
     * Query storage information based on business group id and data stream id
     *
     * @param groupId Business group id
     * @param streamId Data stream id, can be null
     * @return Store information list
     * @apiNote Storage types only support temporarily: HIVE
     */
    List<StorageResponse> listStorage(String groupId, String streamId);

    /**
     * Query stored summary information based on business group id and data stream id, including storage cluster
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return Store information list
     * @apiNote Storage types only support temporarily: HIVE
     */
    List<StorageBriefResponse> listBrief(String groupId, String streamId);

    /**
     * Query the number of undeleted stored information based on business and data stream id
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return Number of stored information
     */
    Integer getCount(String groupId, String streamId);

    /**
     * Paging query storage information based on conditions
     *
     * @param request Paging request
     * @return Store information list
     */
    PageInfo<? extends StorageListResponse> listByCondition(StoragePageRequest request);

    /**
     * Modify data storage information
     *
     * @param storageRequest Information that needs to be modified
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean update(StorageRequest storageRequest, String operator);

    /**
     * Delete the data storage by the given id and storage type.
     *
     * @param id The primary key of the data storage.
     * @param storageType Storage type.
     * @param operator The operator's name.
     * @return Whether succeed
     */
    boolean delete(Integer id, String storageType, String operator);

    /**
     * Modify storage data status
     *
     * @param id Stored id
     * @param status Goal status
     * @param log Modify the description
     */
    void updateStatus(int id, int status, String log);

    /**
     * Logically delete data storage with the given conditions.
     *
     * @param groupId InLong group id to which the data source belongs.
     * @param streamId InLong stream id to which the data source belongs.
     * @param operator The operator's name.
     * @return Whether succeed.
     */
    boolean logicDeleteAll(String groupId, String streamId, String operator);

    /**
     * Physically delete data storage with the given conditions.
     *
     * @param groupId InLong group id.
     * @param streamId InLong stream id.
     * @param operator The operator's name.
     * @return Whether succeed.
     */
    boolean deleteAll(String groupId, String streamId, String operator);

    /**
     * According to the existing data stream ID list, filter out the data stream ID list containing the specified
     * storage type
     *
     * @param groupId Business group id
     * @param storageType Storage type
     * @param streamIdList Data stream ID list
     * @return List of filtered data stream IDs
     */
    List<String> getExistsStreamIdList(String groupId, String storageType, List<String> streamIdList);

    /**
     * According to the data stream id, query the list of storage types owned by it
     *
     * @param groupId Business group id
     * @param streamId Data stream id
     * @return List of storage types
     */
    List<String> getStorageTypeList(String groupId, String streamId);

    /**
     * Save the information modified when the approval is passed
     *
     * @param storageApproveList Data storage approval information
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean updateAfterApprove(List<StorageApproveDTO> storageApproveList, String operator);

}
