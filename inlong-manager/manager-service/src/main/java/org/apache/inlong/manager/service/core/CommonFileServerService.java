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
import org.apache.inlong.manager.common.pojo.commonserver.CommonFileServerInfo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonFileServerListVo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonFileServerPageRequest;

public interface CommonFileServerService {

    int create(CommonFileServerInfo commonFileServerInfo) throws Exception;

    CommonFileServerInfo get(int id) throws Exception;

    void delete(int id) throws Exception;

    CommonFileServerInfo update(CommonFileServerInfo commonFileServerInfo) throws Exception;

    CommonFileServerInfo freeze(int id) throws Exception;

    CommonFileServerInfo unfreeze(int id) throws Exception;

    List<CommonFileServerInfo> getByUser(String user) throws Exception;

    CommonFileServerInfo addVisiblePerson(Integer id, String visiblePerson);

    CommonFileServerInfo deleteVisiblePerson(Integer id, String visiblePerson);

    CommonFileServerInfo addVisibleGroup(Integer id, String visibleGroup);

    CommonFileServerInfo deleteVisibleGroup(Integer id, String visibleGroup);

    PageInfo<CommonFileServerListVo> listByCondition(CommonFileServerPageRequest request) throws Exception;
}
