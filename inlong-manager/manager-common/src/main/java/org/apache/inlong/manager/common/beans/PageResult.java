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

package org.apache.inlong.manager.common.beans;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;

/**
 * Result of the pagination
 */
@ApiModel(value = "Result of the pagination")
public class PageResult<T> {

    @ApiModelProperty(value = "current page")
    private int pageNum;

    @ApiModelProperty(value = "page size")
    private int pageSize;

    @ApiModelProperty(value = "total record size")
    private long totalSize;

    @ApiModelProperty(value = "total pages")
    private int totalPages;

    @ApiModelProperty(value = "data module")
    private List<T> list;

    public int getPageNum() {
        return pageNum;
    }

    public PageResult<T> setPageNum(int pageNum) {
        this.pageNum = pageNum;
        return this;
    }

    public int getPageSize() {
        return pageSize;
    }

    public PageResult<T> setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public PageResult<T> setTotalSize(long totalSize) {
        this.totalSize = totalSize;
        return this;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public PageResult<T> setTotalPages(int totalPages) {
        this.totalPages = totalPages;
        return this;
    }

    public List<T> getList() {
        return list;
    }

    public PageResult<T> setList(List<T> list) {
        this.list = list;
        return this;
    }
}
