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

package org.apache.inlong.manager.common.util;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.beans.PageResult;

/**
 * Pagination utils
 */
public class PageUtils {

    /**
     * Wrapping paging information
     *
     * @param pageInfo page info need to wrap
     * @return page result
     */
    public static <T> PageResult<T> getPageResult(PageInfo<T> pageInfo) {
        PageResult<T> pageResult = new PageResult<>();
        pageResult.setPageNum(pageInfo.getPageNum());
        pageResult.setPageSize(pageInfo.getPageSize());
        pageResult.setTotalSize(pageInfo.getTotal());
        pageResult.setTotalPages(pageInfo.getPages());
        pageResult.setList(pageInfo.getList());
        return pageResult;
    }

    public static <T, E> PageResult<T> getPageResult(Page<E> page, Page.Function<E, T> function) {
        PageInfo<T> pageInfo = page.toPageInfo(function);
        pageInfo.setTotal(page.getTotal());
        PageResult<T> pageResult = new PageResult<>();
        pageResult.setPageNum(pageInfo.getPageNum());
        pageResult.setPageSize(pageInfo.getPageSize());
        pageResult.setTotalSize(pageInfo.getTotal());
        pageResult.setTotalPages(pageInfo.getPages());
        pageResult.setList(pageInfo.getList());
        return pageResult;
    }
}
