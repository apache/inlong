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

/**
 * The service interface for common file server.
 */
public interface CommonFileServerService {

    /**
     * Create common file server.
     *
     * @param commonFileServerInfo common file server info.
     * @return the id of common file server info.
     */
    int create(CommonFileServerInfo commonFileServerInfo) throws Exception;

    /**
     * Get common file server info.
     *
     * @param id common file server info id.
     * @return the common file server info.
     */
    CommonFileServerInfo get(int id) throws Exception;

    /**
     * Delete common file server info by id.
     *
     * @param id common file server info id.
     */
    void delete(int id) throws Exception;

    /**
     * Update common file server info.
     *
     * @param commonFileServerInfo common file server info.
     * @return the updated common file server info .
     */
    CommonFileServerInfo update(CommonFileServerInfo commonFileServerInfo) throws Exception;

    /**
     * Freeze Common file server.
     *
     * @param id the id of common file server.
     * @return the updated common file server info.
     */
    CommonFileServerInfo freeze(int id);

    /**
     * Unfreeze Common file server.
     *
     * @param id the id of file.
     * @return the updated common file server info.
     */
    CommonFileServerInfo unfreeze(int id);

    /**
     * Get common file server info list by username.
     *
     * @param user the username for common file server.
     * @return common file server info list.
     */
    List<CommonFileServerInfo> getByUser(String user);

    /**
     * Add visible person for common file server.
     *
     * @param id common database server info id.
     * @param visiblePerson the visible persion of common file server.
     * @return the updated common file server info.
     */
    CommonFileServerInfo addVisiblePerson(Integer id, String visiblePerson);

    /**
     * Delete visible person for common file server.
     *
     * @param id common database server info id.
     * @param visiblePerson the visible person of common database server.
     * @return the updated common file server info.
     */
    CommonFileServerInfo deleteVisiblePerson(Integer id, String visiblePerson);

    /**
     * Add visible group for common file server.
     *
     * @param id common database server info id.
     * @param visibleGroup the visible group of common file server.
     * @return the updated common file server info.
     */
    CommonFileServerInfo addVisibleGroup(Integer id, String visibleGroup);

    /**
     * Delete visible group for common file server.
     *
     * @param id common file server info id.
     * @param visibleGroup the visible group of common file server.
     * @return the updated common file server info.
     */
    CommonFileServerInfo deleteVisibleGroup(Integer id, String visibleGroup);

    /**
     * Query common file server for list by condition
     *
     * @param request The common file server request of query condition
     * @return The result of query
     */
    PageInfo<CommonFileServerListVo> listByCondition(CommonFileServerPageRequest request) throws Exception;
}
