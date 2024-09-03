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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AuditMetric {

    private Long successPack = 0L;
    private Long failedPack = 0L;
    private Long totalMsg = 0L;
    private Long memorySize = 0L;

    public void addSuccessPack(long successPack) {
        this.successPack += successPack;
    }

    public void addFailedPack(long failedPack) {
        this.failedPack += failedPack;
    }

    public void addTotalMsg(long totalMsg) {
        this.totalMsg += totalMsg;
    }

    public void addMemorySize(long memorySize) {
        this.memorySize += memorySize;
    }

    public void reset() {
        successPack = 0L;
        failedPack = 0L;
        totalMsg = 0L;
        memorySize = 0L;
    }
}
