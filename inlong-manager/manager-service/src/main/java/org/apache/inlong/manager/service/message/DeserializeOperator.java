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

package org.apache.inlong.manager.service.message;

import org.apache.inlong.common.enums.MessageWrapType;
import org.apache.inlong.manager.pojo.consume.DisplayMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import java.util.List;
import java.util.Map;

/**
 * Deserialize of message operator
 */
public interface DeserializeOperator {

    /**
     * Determines whether the current instance matches the specified type.
     */
    boolean accept(MessageWrapType type);

    default List<DisplayMessage> decodeMsg(InlongStreamInfo streamInfo,
            byte[] msgBytes, Map<String, String> headers) throws Exception {
        return null;
    }
}
