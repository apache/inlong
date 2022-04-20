/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.manager.common.thirdparty.sort;

import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.sink.SinkInfo;

import java.util.List;

/**
 * Interface of sink info generator.
 * Use to generate user-defined sink info.
 */
public interface SinkInfoGenerator {

    /**
     * Method to create customized sink info.
     *
     * @param sourceResponse sourceResponse
     * @param sinkResponse sinkResponse
     * @param sinkFields sinkFields
     * @return Customized sink info
     */
    SinkInfo createSinkInfo(
            SourceResponse sourceResponse,
            SinkResponse sinkResponse,
            List<FieldInfo> sinkFields);
}
