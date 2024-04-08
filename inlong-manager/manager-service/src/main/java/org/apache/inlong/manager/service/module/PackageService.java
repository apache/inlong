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

package org.apache.inlong.manager.service.module;

import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.module.PackagePageRequest;
import org.apache.inlong.manager.pojo.module.PackageRequest;
import org.apache.inlong.manager.pojo.module.PackageResponse;
import org.apache.inlong.manager.pojo.user.UserInfo;

public interface PackageService {

    /**
     * Save inlong package information.
     *
     * @param request Inlong package information.
     * @param operator The name of operator.
     * @return Id after successful save.
     */
    Integer save(PackageRequest request, String operator);

    /**
     * Modify inlong module information
     *
     * @param request Information that needs to be modified
     * @param operator Operator's name
     * @return whether succeed
     */
    Boolean update(PackageRequest request, String operator);

    /**
     * Get package info based on package id
     *
     * @param id package id
     * @param opInfo userinfo of operator
     * @return detail of package config
     */
    PackageResponse get(Integer id, UserInfo opInfo);

    /**
     * Paging query package information based on conditions.
     *
     * @param request paging request.
     * @return package list
     */
    PageResult<PackageResponse> listByCondition(PackagePageRequest request);

    /**
     * Delete the package config by the given id.
     *
     * @param id The primary key of the package.
     * @param operator Operator's name
     * @return Whether succeed
     */
    Boolean delete(Integer id, String operator);

}
