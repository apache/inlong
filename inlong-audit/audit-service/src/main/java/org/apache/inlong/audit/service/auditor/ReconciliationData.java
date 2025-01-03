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

package org.apache.inlong.audit.service.auditor;

import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.utils.AuditUtils;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class ReconciliationData {

    public StatData srcData;
    public StatData destData;

    public double getDiffRatio() {
        if (srcData == null && destData == null) {
            return 0;
        }
        if (srcData == null || destData == null) {
            return 1;
        }
        return AuditUtils.calculateDiffRatio(srcData.getCount(), destData.getCount());
    }

    public List<StatData> getCombinedData() {
        List<StatData> result = new ArrayList<>();
        if (srcData != null) {
            result.add(srcData);
        }
        if (destData != null) {
            result.add(destData);
        }
        return result;
    }

    public boolean isNotEmpty() {
        return srcData != null || destData != null;
    }
}
