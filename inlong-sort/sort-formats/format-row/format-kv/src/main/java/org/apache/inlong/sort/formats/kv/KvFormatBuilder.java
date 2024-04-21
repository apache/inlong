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

package org.apache.inlong.sort.formats.kv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.TextFormatBuilder;

import org.apache.flink.table.descriptors.DescriptorProperties;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;

/**
 * The builder for the serializers and deserializers for kv formats.
 */
public abstract class KvFormatBuilder<T extends KvFormatBuilder> extends TextFormatBuilder<T> {

    protected char entryDelimiter = DEFAULT_ENTRY_DELIMITER;
    protected char kvDelimiter = DEFAULT_KV_DELIMITER;

    public KvFormatBuilder(RowFormatInfo rowFormatInfo) {
        super(rowFormatInfo);
    }

    @SuppressWarnings("unchecked")
    public T setEntryDelimiter(char entryDelimiter) {
        this.entryDelimiter = entryDelimiter;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setKvDelimiter(char kvDelimiter) {
        this.kvDelimiter = kvDelimiter;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T configure(DescriptorProperties descriptorProperties) {
        super.configure(descriptorProperties);

        descriptorProperties.getOptionalCharacter(FORMAT_ENTRY_DELIMITER)
                .ifPresent(this::setEntryDelimiter);
        descriptorProperties.getOptionalCharacter(FORMAT_KV_DELIMITER)
                .ifPresent(this::setKvDelimiter);

        return (T) this;
    }
}
