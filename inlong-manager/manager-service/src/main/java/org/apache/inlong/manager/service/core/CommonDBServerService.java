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
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerInfo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerListVo;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerPageRequest;

public interface CommonDBServerService {

    int create(CommonDbServerInfo commonDbServerInfo) throws Exception;

    CommonDbServerInfo get(int id) throws Exception;

    void delete(int id) throws Exception;

    CommonDbServerInfo update(CommonDbServerInfo commonDbServerInfo) throws Exception;

    List<CommonDbServerInfo> getByUser(String user) throws Exception;

    CommonDbServerInfo addVisiblePerson(Integer id, String visiblePerson);

    CommonDbServerInfo deleteVisiblePerson(Integer id, String visiblePerson);

    CommonDbServerInfo addVisibleGroup(Integer id, String visibleGroup);

    CommonDbServerInfo deleteVisibleGroup(Integer id, String visibleGroup);

    PageInfo<CommonDbServerListVo> listByCondition(CommonDbServerPageRequest request) throws Exception;
}
