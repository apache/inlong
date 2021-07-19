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

package org.apache.inlong.sort.protocol;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class DataFlowStorageInfo {

    public enum StorageType {
        ZK,
        HDFS
    }

    @JsonProperty("storage_type")
    private final StorageType storageType;

    @JsonProperty("path")
    private final String path;

    @JsonCreator
    public DataFlowStorageInfo(
            @JsonProperty("storage_type") StorageType storageType,
            @JsonProperty("path") String path) {
        this.storageType = Preconditions.checkNotNull(storageType);
        this.path = Preconditions.checkNotNull(path);
    }

    @JsonProperty("storage_type")
    public StorageType getStorageType() {
        return storageType;
    }

    @JsonProperty("path")
    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataFlowStorageInfo that = (DataFlowStorageInfo) o;

        return Objects.equals(storageType, that.storageType)
                && Objects.equals(path, that.path);
    }
}
