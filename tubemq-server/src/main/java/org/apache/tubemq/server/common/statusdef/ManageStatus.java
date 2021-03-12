/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.common.statusdef;

public enum ManageStatus {

    STATUS_MANAGE_UNDEFINED(-2, "Undefined."),
    STATUS_MANAGE_APPLY(1, "Apply."),
    STATUS_MANAGE_ONLINE(5, "Online."),
    STATUS_MANAGE_ONLINE_NOT_WRITE(6, "Online with not write"),
    STATUS_MANAGE_ONLINE_NOT_READ(7, "Online with not read"),
    STATUS_MANAGE_OFFLINE(9, "Offline");

    private int code;
    private String description;


    ManageStatus(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public static ManageStatus valueOf(int code) {
        for (ManageStatus status : ManageStatus.values()) {
            if (status.getCode() == code) {
                return status;
            }
        }
        throw new IllegalArgumentException(String.format(
                "unknown broker manage status code %s", code));
    }
}
