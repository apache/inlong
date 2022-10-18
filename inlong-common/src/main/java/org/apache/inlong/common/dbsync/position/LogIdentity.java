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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.inlong.common.util.JsonUtils.JSONObject;

import java.net.InetSocketAddress;

public class LogIdentity extends Position implements Comparable<LogIdentity>{

    private static final long serialVersionUID = 5530225131455662581L;
    private InetSocketAddress sourceAddress;
    private Long              slaveId;

    public LogIdentity(){
    }

    public LogIdentity(InetSocketAddress sourceAddress, Long slaveId){
        this.sourceAddress = sourceAddress;
        this.slaveId = slaveId;
    }

    /*add deep copy*/
    public LogIdentity(LogIdentity other){
        this.sourceAddress = other.sourceAddress;
        this.slaveId = other.slaveId;
    }

    public LogIdentity(JSONObject obj){
        this.sourceAddress = new InetSocketAddress(obj.getString("sourceIp"), obj.getIntValue("sourcePort"));
        this.slaveId = obj.getLong("slaveId");
    }

    public InetSocketAddress getSourceAddress() {
        return sourceAddress;
    }

    public void setSourceAddress(InetSocketAddress sourceAddress) {
        this.sourceAddress = sourceAddress;
    }

    public Long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(Long slaveId) {
        this.slaveId = slaveId;
    }

    public JSONObject getJsonObj(){
        JSONObject obj = new JSONObject();
        obj.put("sourceIp", sourceAddress.getAddress().getHostAddress());
        obj.put("sourcePort", sourceAddress.getPort());
        obj.put("slaveId", slaveId);
        return obj;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((slaveId == null) ? 0 : slaveId.hashCode());
        result = prime * result + ((sourceAddress == null) ? 0 : sourceAddress.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        LogIdentity other = (LogIdentity) obj;
        if (slaveId == null) {
            if (other.slaveId != null) return false;
        } else if (slaveId.longValue() != (other.slaveId.longValue())) return false;
        if (sourceAddress == null) {
            if (other.sourceAddress != null) return false;
        } else if (!sourceAddress.equals(other.sourceAddress)) return false;
        return true;
    }

    @Override
    public int compareTo(LogIdentity otherPos) {
        String currentAddress = (sourceAddress == null ? "" : sourceAddress.getHostName());
        String otherAddress = "";
        Long otherSlaveId = 0L;
        Long localSlaveId = (slaveId == null ? 0L : slaveId);
        if (otherPos != null) {
            otherAddress = (otherPos.getSourceAddress() == null ? "" :
                    otherPos.getSourceAddress().getHostName());
            otherSlaveId = (otherPos.getSlaveId() == null ? 0L : otherPos.getSlaveId());
        }
        int cmp = currentAddress.compareTo(otherAddress);
        if (cmp == 0) {
            cmp = (int)(localSlaveId - otherSlaveId);
        }
        return cmp;
    }

}

