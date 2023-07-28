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

public class TenantClusterTagEntity implements Serializable {

    private Integer id;

    private String tenant;

    private String clusterTag;

    private Integer isDeleted;

    private String creator;

    private String modifier;

    private Date createTime;

    private Date modifyTime;

    private Integer version;

    private static final long serialVersionUID = 1L;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant == null ? null : tenant.trim();
    }

    public String getClusterTag() {
        return clusterTag;
    }

    public void setClusterTag(String clusterTag) {
        this.clusterTag = clusterTag == null ? null : clusterTag.trim();
    }

    public Integer getIsDeleted() {
        return isDeleted;
    }

    public void setIsDeleted(Integer isDeleted) {
        this.isDeleted = isDeleted;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator == null ? null : creator.trim();
    }

    public String getModifier() {
        return modifier;
    }

    public void setModifier(String modifier) {
        this.modifier = modifier == null ? null : modifier.trim();
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
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
        TenantClusterTagEntity other = (TenantClusterTagEntity) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
                && (this.getTenant() == null ? other.getTenant() == null : this.getTenant().equals(other.getTenant()))
                && (this.getClusterTag() == null ? other.getClusterTag() == null
                        : this.getClusterTag().equals(other.getClusterTag()))
                && (this.getIsDeleted() == null ? other.getIsDeleted() == null
                        : this.getIsDeleted().equals(other.getIsDeleted()))
                && (this.getCreator() == null ? other.getCreator() == null
                        : this.getCreator().equals(other.getCreator()))
                && (this.getModifier() == null ? other.getModifier() == null
                        : this.getModifier().equals(other.getModifier()))
                && (this.getCreateTime() == null ? other.getCreateTime() == null
                        : this.getCreateTime().equals(other.getCreateTime()))
                && (this.getModifyTime() == null ? other.getModifyTime() == null
                        : this.getModifyTime().equals(other.getModifyTime()))
                && (this.getVersion() == null ? other.getVersion() == null
                        : this.getVersion().equals(other.getVersion()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getTenant() == null) ? 0 : getTenant().hashCode());
        result = prime * result + ((getClusterTag() == null) ? 0 : getClusterTag().hashCode());
        result = prime * result + ((getIsDeleted() == null) ? 0 : getIsDeleted().hashCode());
        result = prime * result + ((getCreator() == null) ? 0 : getCreator().hashCode());
        result = prime * result + ((getModifier() == null) ? 0 : getModifier().hashCode());
        result = prime * result + ((getCreateTime() == null) ? 0 : getCreateTime().hashCode());
        result = prime * result + ((getModifyTime() == null) ? 0 : getModifyTime().hashCode());
        result = prime * result + ((getVersion() == null) ? 0 : getVersion().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", id=").append(id);
        sb.append(", tenant=").append(tenant);
        sb.append(", clusterTag=").append(clusterTag);
        sb.append(", isDeleted=").append(isDeleted);
        sb.append(", creator=").append(creator);
        sb.append(", modifier=").append(modifier);
        sb.append(", createTime=").append(createTime);
        sb.append(", modifyTime=").append(modifyTime);
        sb.append(", version=").append(version);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}