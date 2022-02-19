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

import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.enums.StorageType;
import org.apache.inlong.manager.common.pojo.datastorage.StorageListResponse;
import org.apache.inlong.manager.common.pojo.datastorage.StorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.dao.entity.StorageEntity;

import java.util.function.Supplier;

/**
 * Interface of the storage operation
 */
public interface StorageOperation {

    /**
     * Determines whether the current instance matches the specified type.
     */
    Boolean accept(StorageType storageType);

    /**
     * Save the storage info.
     *
     * @param request The request of the storage.
     * @param operator The operator name.
     * @return Storage id after saving.
     */
    default Integer saveOpt(StorageRequest request, String operator) {
        return null;
    }

    /**
     * Save storage fields via the storage request.
     *
     * @param request Storage request.
     */
    default void saveFieldOpt(StorageRequest request) {
    }

    /**
     * Get storage info by storage type and storage id.
     *
     * @param storageType Storage type.
     * @param id Storage id.
     * @return Storage info.
     */
    StorageResponse getById(String storageType, Integer id);

    /**
     * Get the target from the given entity.
     *
     * @param entity Get field value from the entity.
     * @param target Encapsulate value to the target.
     * @param <T> Type of the target.
     * @return Target after encapsulating.
     */
    <T> T getFromEntity(StorageEntity entity, Supplier<T> target);

    /**
     * Get storage list response from the given storage entity page.
     *
     * @param entityPage The given entity page.
     * @return Storage list response.
     */
    default PageInfo<? extends StorageListResponse> getPageInfo(Page<StorageEntity> entityPage) {
        return new PageInfo<>();
    }

    /**
     * Update the storage info.
     *
     * @param request Request of update.
     * @param operator Operator's name.
     */
    void updateOpt(StorageRequest request, String operator);

    /**
     * Update the storage fields.
     * <p/>If `onlyAdd` is <code>true</code>, only adding is allowed, modification and deletion are not allowed,
     * and the order of existing fields cannot be changed
     *
     * @param onlyAdd Whether to add fields only.
     * @param request The update request.
     */
    void updateFieldOpt(Boolean onlyAdd, StorageRequest request);

}
