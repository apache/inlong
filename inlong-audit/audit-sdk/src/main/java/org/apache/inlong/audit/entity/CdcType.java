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

package org.apache.inlong.audit.entity;

import lombok.Getter;

/**
 * CDC (Change Data Capture) operation types.
 */
@Getter
public enum CdcType {

    INSERT(" insert ", "写入"),
    DELETE(" delete ", "删除"),
    UPDATE_BEFORE("update before", "update before"),
    UPDATE_AFTER("update after", "update after");

    private final String nameInEnglish;
    private final String nameInChinese;

    CdcType(String nameInEnglish, String nameInChinese) {
        this.nameInEnglish = nameInEnglish;
        this.nameInChinese = nameInChinese;
    }
}
