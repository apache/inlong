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

package org.apache.inlong.audit.util;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class AuditConfig {

    private static final Logger logger = LoggerFactory.getLogger(AuditConfig.class);
    private static String FILE_PATH = "./inLong-audit/";
    private static final int FILE_SIZE = 100 * 1024 * 1024;
    private static final int MAX_CACHE_ROWS = 1000000;
    private static final int MIN_CACHE_ROWS = 100;

    private String filePath;
    private int maxCacheRow;
    private int maxFileSize = FILE_SIZE;
    private String disasterFileName = "disaster.data";
    private int socketTimeout = 30000;
    private int retryTimes = 2;

    public AuditConfig(String filePath, int maxCacheRow) {
        if (filePath == null || filePath.length() == 0) {
            this.filePath = FILE_PATH;
        } else {
            this.filePath = filePath;
        }
        if (maxCacheRow < MIN_CACHE_ROWS) {
            this.maxCacheRow = MAX_CACHE_ROWS;
        } else {
            this.maxCacheRow = maxCacheRow;
        }
    }

    public AuditConfig() {
        this.filePath = FILE_PATH;
        this.maxCacheRow = MAX_CACHE_ROWS;
    }

    public String getDisasterFile() {
        return filePath + "/" + disasterFileName;
    }

}
