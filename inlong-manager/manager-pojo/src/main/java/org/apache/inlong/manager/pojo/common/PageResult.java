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

package org.apache.inlong.manager.pojo.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.github.pagehelper.Page;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@ApiModel("Paginated query results")
public final class PageResult<T> implements Serializable {

    @ApiModelProperty(value = "data record", required = true)
    private List<T> list;

    @ApiModelProperty(value = "The total number of items matching the filter criteria", required = true)
    private Long total;

    @ApiModelProperty(value = "pageSize", required = true)
    private Integer pageSize;

    @ApiModelProperty(value = "pageNum", required = true)
    private Integer pageNum;

    public PageResult() {
    }

    public PageResult(List<T> list, Long total) {
        this.list = list;
        this.total = total;
    }

    public PageResult(List<T> list, Long total, Integer pageNum, Integer pageSize) {
        this.list = list;
        this.total = total;
        this.pageNum = pageNum;
        this.pageSize = pageSize;
    }

    public PageResult(Long total) {
        this.list = new ArrayList<>();
        this.total = total;
    }

    public PageResult(List<T> list) {
        this.list = list;
        this.total = (long) list.size();
    }

    public <R> PageResult<R> map(Function<? super T, ? extends R> mapper) {
        List<R> newList = list.stream().map(mapper).collect(Collectors.toList());
        return new PageResult<>(newList, total, pageNum, pageSize);
    }

    public PageResult<T> foreach(Consumer<? super T> action) {
        list.forEach(action);
        return this;
    }

    public static <T> PageResult<T> fromPage(Page<T> page) {
        return new PageResult<>(page.getResult(), page.getTotal(), page.getPageNum(), page.getPageSize());
    }

    public static <T> PageResult<T> empty() {
        return new PageResult<>(0L);
    }

    public static <T> PageResult<T> empty(Long total) {
        return new PageResult<>(total);
    }

}