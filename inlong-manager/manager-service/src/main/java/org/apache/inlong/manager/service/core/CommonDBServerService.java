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

/**
 * The service interface for common database server.
 */
public interface CommonDBServerService {

    /**
     * Save common database server info.
     *
     * @param commonDbServerInfo common database server info.
     * @return the id of common database server info.
     */
    int save(CommonDbServerInfo commonDbServerInfo) throws Exception;

    /**
     * Get common database server info by id.
     *
     * @param id common database server info id.
     * @return the common database server info.
     */
    CommonDbServerInfo get(int id) throws Exception;

    /**
     * Delete common database server info by id.
     *
     * @param id common database server info id.
     */
    void delete(int id) throws Exception;

    /**
     * Update common database server info.
     *
     * @param commonDbServerInfo common database server info.
     * @return the updated common database server info .
     */
    CommonDbServerInfo update(CommonDbServerInfo commonDbServerInfo) throws Exception;

    /**
     * Get common database server info list by username.
     *
     * @param user the username of database.
     * @return common database server info list.
     */
    List<CommonDbServerInfo> getByUser(String user) throws Exception;

    /**
     * Add visible persion for common database server.
     *
     * @param id common database server info id.
     * @param visiblePerson the visible person of common database server.
     * @return the updated common database server info.
     */
    CommonDbServerInfo addVisiblePerson(Integer id, String visiblePerson);

    /**
     * Delete visible persion for common database server.
     *
     * @param id common database server info id.
     * @param visiblePerson the visiblePerson of common database server.
     * @return the updated common database server info.
     */
    CommonDbServerInfo deleteVisiblePerson(Integer id, String visiblePerson);

    /**
     * Add visible group for common database server.
     *
     * @param id common database server info id.
     * @param visibleGroup the visible group of common database server.
     * @return the updated common database server info.
     */
    CommonDbServerInfo addVisibleGroup(Integer id, String visibleGroup);

    /**
     * Delete visiable group for common database server.
     *
     * @param id common database server info id.
     * @param visibleGroup the visible group of common database server.
     * @return the updated common database server info.
     */
    CommonDbServerInfo deleteVisibleGroup(Integer id, String visibleGroup);

    /**
     * Query common database server for list by condition
     *
     * @param request The common database server request of query condition
     * @return The result of query
     */
    PageInfo<CommonDbServerListVo> listByCondition(CommonDbServerPageRequest request) throws Exception;
}
