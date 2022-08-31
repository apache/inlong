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

package org.apache.inlong.manager.service.consume;

import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.pojo.consume.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consume.InlongConsumeRequest;

/**
 * Interface of the inlong consume operator.
 */
public interface InlongConsumeOperator {

    /**
     * Get the MQ type.
     *
     * @return MQ type string
     */
    String getMQType();

    /**
     * Determines whether the current instance matches the specified type.
     */
    Boolean accept(String mqType);

    void setTopicInfo(InlongConsumeRequest consumeRequest);

    void saveOpt(InlongConsumeRequest consumeRequest, String operator);

    void updateOpt(InlongConsumeRequest consumeRequest, InlongConsumeEntity consumeEntity, String operator);

    InlongConsumeInfo getFromEntity(InlongConsumeEntity entity);
}
