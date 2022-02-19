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

import org.apache.inlong.manager.common.enums.BizErrorCodeEnum;
import org.apache.inlong.manager.common.enums.StorageType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Factory for {@link StorageOperation}.
 */
@Service
public class StorageOperationFactory {

    @Autowired
    private List<StorageOperation> storageOperationList;

    /**
     * Get a storage operation instance via the given storageType
     */
    public StorageOperation getInstance(StorageType storageType) {
        Optional<StorageOperation> instance = storageOperationList.stream()
                .filter(inst -> inst.accept(storageType))
                .findFirst();
        if (!instance.isPresent()) {
            throw new BusinessException(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORT,
                    String.format(BizErrorCodeEnum.STORAGE_TYPE_NOT_SUPPORT.getMessage(), storageType));
        }
        return instance.get();
    }

}
