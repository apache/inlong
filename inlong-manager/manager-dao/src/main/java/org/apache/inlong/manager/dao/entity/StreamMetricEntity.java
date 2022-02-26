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

package org.apache.inlong.manager.dao.entity;

import java.io.Serializable;
import java.util.Date;

public class StreamMetricEntity implements Serializable {
    private Integer id;

    private String ip;

    private String version;

    private String inlongStreamId;

    private String inlongGroupId;

    private String componentName;

    private String metricName;

    private Date reportTime;

    private Date modifyTime;

    private String metricInfo;

    private static final long serialVersionUID = 1L;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip == null ? null : ip.trim();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version == null ? null : version.trim();
    }

    public String getInlongStreamId() {
        return inlongStreamId;
    }

    public void setInlongStreamId(String inlongStreamId) {
        this.inlongStreamId = inlongStreamId == null ? null : inlongStreamId.trim();
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public void setInlongGroupId(String inlongGroupId) {
        this.inlongGroupId = inlongGroupId == null ? null : inlongGroupId.trim();
    }

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName == null ? null : componentName.trim();
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName == null ? null : metricName.trim();
    }

    public Date getReportTime() {
        return reportTime;
    }

    public void setReportTime(Date reportTime) {
        this.reportTime = reportTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getMetricInfo() {
        return metricInfo;
    }

    public void setMetricInfo(String metricInfo) {
        this.metricInfo = metricInfo == null ? null : metricInfo.trim();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (getClass() != that.getClass()) {
            return false;
        }
        StreamMetricEntity other = (StreamMetricEntity) that;
        return (this.getId() == null ? other.getId() == null
                : this.getId().equals(other.getId()))
            && (this.getIp() == null ? other.getIp() == null
                : this.getIp().equals(other.getIp()))
            && (this.getVersion() == null ? other.getVersion() == null
                : this.getVersion().equals(other.getVersion()))
            && (this.getInlongStreamId() == null ? other.getInlongStreamId() == null
                : this.getInlongStreamId().equals(other.getInlongStreamId()))
            && (this.getInlongGroupId() == null ? other.getInlongGroupId() == null
                : this.getInlongGroupId().equals(other.getInlongGroupId()))
            && (this.getComponentName() == null ? other.getComponentName() == null
                : this.getComponentName().equals(other.getComponentName()))
            && (this.getMetricName() == null ? other.getMetricName() == null
                : this.getMetricName().equals(other.getMetricName()))
            && (this.getReportTime() == null ? other.getReportTime() == null
                : this.getReportTime().equals(other.getReportTime()))
            && (this.getModifyTime() == null ? other.getModifyTime() == null
                : this.getModifyTime().equals(other.getModifyTime()))
            && (this.getMetricInfo() == null ? other.getMetricInfo() == null
                : this.getMetricInfo().equals(other.getMetricInfo()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getIp() == null) ? 0 : getIp().hashCode());
        result = prime * result + ((getVersion() == null) ? 0 : getVersion().hashCode());
        result = prime * result + ((getInlongStreamId() == null) ? 0 : getInlongStreamId().hashCode());
        result = prime * result + ((getInlongGroupId() == null) ? 0 : getInlongGroupId().hashCode());
        result = prime * result + ((getComponentName() == null) ? 0 : getComponentName().hashCode());
        result = prime * result + ((getMetricName() == null) ? 0 : getMetricName().hashCode());
        result = prime * result + ((getReportTime() == null) ? 0 : getReportTime().hashCode());
        result = prime * result + ((getModifyTime() == null) ? 0 : getModifyTime().hashCode());
        result = prime * result + ((getMetricInfo() == null) ? 0 : getMetricInfo().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", id=").append(id);
        sb.append(", ip=").append(ip);
        sb.append(", version=").append(version);
        sb.append(", inlongStreamId=").append(inlongStreamId);
        sb.append(", inlongGroupId=").append(inlongGroupId);
        sb.append(", componentName=").append(componentName);
        sb.append(", metricName=").append(metricName);
        sb.append(", reportTime=").append(reportTime);
        sb.append(", modifyTime=").append(modifyTime);
        sb.append(", metricInfo=").append(metricInfo);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}