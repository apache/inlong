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

package org.apache.inlong.sdk.transform.decode;

import org.apache.inlong.sdk.transform.process.Context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * KvSourceData
 */
public class KvSourceData extends AbstractSourceData {

    private List<Map<String, String>> rows = new ArrayList<>();

    private Map<String, String> currentRow;

    public KvSourceData(Context context) {
        this.context = context;
    }

    public void putField(String fieldName, String fieldValue) {
        this.currentRow.put(fieldName, fieldValue);
    }

    public void addRow() {
        this.currentRow = new HashMap<>();
        rows.add(currentRow);
    }

    @Override
    public int getRowCount() {
        return this.rows.size();
    }

    @Override
    public String getField(int rowNum, String fieldName) {
        if (rowNum >= this.rows.size()) {
            return null;
        }
        if (isContextField(fieldName)) {
            return getContextField(fieldName);
        }
        Map<String, String> targetRow = this.rows.get(rowNum);
        return targetRow.get(fieldName);
    }
}
