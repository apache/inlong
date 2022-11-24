/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.base.enums;

/**
 * A enum class for metrics read phase
 */
public enum ReadPhase {

    /**
     * This indicator represents the snapshot phase during the read phase
     */
    SNAPSHOT_PHASE("snapshot", 1L),
    /**
     * This indicator represents the incremental phase during the read phase
     */
    INCREASE_PHASE("incremental", 2L);

    private String phase;
    private Long value;

    ReadPhase(String phase, Long value) {
        this.phase = phase;
        this.value = value;
    }

    public String getPhase() {
        return phase;
    }

    public void setPhase(String phase) {
        this.phase = phase;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
