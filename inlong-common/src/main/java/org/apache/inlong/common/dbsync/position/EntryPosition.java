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

package org.apache.inlong.common.dbsync.position;

import org.apache.inlong.common.util.JsonUtils.JSONObject;

import java.util.Objects;

public class EntryPosition extends TimePosition implements Comparable<EntryPosition> {

    public static final int EVENTIDENTITY_SEGMENT = 3;
    public static final char EVENTIDENTITY_SPLIT = (char) 5;
    private static final long serialVersionUID = 81432665066427482L;
    private boolean included = false;
    private String journalName;
    private Long position;
    private Long serverId;

    public EntryPosition() {
        super(null);
    }

    public EntryPosition(Long timestamp) {
        this(null, null, timestamp);
    }

    public EntryPosition(String journalName, Long position) {
        this(journalName, position, null);
    }

    public EntryPosition(String journalName, Long position, Long timestamp) {
        super(timestamp);
        this.journalName = journalName;
        this.position = position;
    }

    public EntryPosition(String journalName, Long position, Long timestamp, Long serverId) {
        this(journalName, position, timestamp);
        this.serverId = serverId;
    }

    /*add deep copy*/
    public EntryPosition(EntryPosition other) {
        super(other.timestamp);
        this.journalName = other.journalName;
        this.position = other.position;
        this.serverId = other.serverId;
        this.included = other.included;
    }

    public EntryPosition(JSONObject obj) {
        super(obj.getLong("timestamp"));
        this.journalName = obj.getString("journalName");
        this.position = obj.getLong("position");
        this.included = obj.getBoolean("included");
        this.serverId = obj.getLong("serverId");
    }

    public String getJournalName() {
        return journalName;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public Long getPosition() {
        return position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public boolean isIncluded() {
        return included;
    }

    public void setIncluded(boolean included) {
        this.included = included;
    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
    }

    public JSONObject getJsonObj() {
        JSONObject obj = new JSONObject();
        obj.put("journalName", this.journalName);
        obj.put("position", this.position);
        obj.put("included", this.included);
        obj.put("timestamp", this.timestamp);
        obj.put("serverId", this.serverId);
        return obj;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((journalName == null) ? 0 : journalName.hashCode());
        result = prime * result + ((position == null) ? 0 : position.hashCode());
        // please note when use equals
        result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (!(obj instanceof EntryPosition)) {
            return false;
        }
        EntryPosition other = (EntryPosition) obj;
        if (journalName == null) {
            if (other.journalName != null) {
                return false;
            }
        } else if (!journalName.equals(other.journalName)) {
            return false;
        }
        if (position == null) {
            if (other.position != null) {
                return false;
            }
        } else if (!position.equals(other.position)) {
            return false;
        }
        // please note when use equals
        if (timestamp == null) {
            if (other.timestamp != null) {
                return false;
            }
        } else if (!timestamp.equals(other.timestamp)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(EntryPosition o) {
        if (timestamp != null && o.timestamp != null) {
            if (!Objects.equals(timestamp, o.timestamp)) {
                return (int) (timestamp - o.timestamp);
            }
        }

        final int val = journalName.compareTo(o.journalName);
        if (val == 0) {
            return (int) (position - o.position);
        }
        return val;
    }

}
