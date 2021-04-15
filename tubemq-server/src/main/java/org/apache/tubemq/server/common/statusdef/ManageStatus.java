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

import org.apache.tubemq.corebase.utils.Tuple2;


public enum ManageStatus {

    STATUS_MANAGE_UNDEFINED(-2, "-", false, false),
    STATUS_MANAGE_APPLY(1, "draft", false, false),
    STATUS_MANAGE_ONLINE(5, "online", true, true),
    STATUS_MANAGE_ONLINE_NOT_WRITE(6, "only-read", false, true),
    STATUS_MANAGE_ONLINE_NOT_READ(7, "only-write", true, false),
    STATUS_MANAGE_OFFLINE(9, "offline", false, false);

    private int code;
    private String description;
    private boolean isAcceptPublish;
    private boolean isAcceptSubscribe;


    ManageStatus(int code, String description,
                 boolean isAcceptPublish,
                 boolean isAcceptSubscribe) {
        this.code = code;
        this.description = description;
        this.isAcceptPublish = isAcceptPublish;
        this.isAcceptSubscribe = isAcceptSubscribe;
    }

    public boolean isOnlineStatus() {
        return (this == ManageStatus.STATUS_MANAGE_ONLINE
                || this == ManageStatus.STATUS_MANAGE_ONLINE_NOT_WRITE
                || this == ManageStatus.STATUS_MANAGE_ONLINE_NOT_READ);
    }

    public boolean isApplied() {
        return (this.code > ManageStatus.STATUS_MANAGE_APPLY.getCode());
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public Tuple2<Boolean, Boolean> getPubSubStatus() {
        return new Tuple2<>(isAcceptPublish, isAcceptSubscribe);
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
