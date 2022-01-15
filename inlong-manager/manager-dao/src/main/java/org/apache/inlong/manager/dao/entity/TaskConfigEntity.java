package org.apache.inlong.manager.dao.entity;

import java.io.Serializable;

public class TaskConfigEntity implements Serializable {
    private Integer id;

    private String name;

    private String type;

    private String inlongGroupId;

    private String inlongStreamId;

    private static final long serialVersionUID = 1L;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type == null ? null : type.trim();
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public void setInlongGroupId(String inlongGroupId) {
        this.inlongGroupId = inlongGroupId == null ? null : inlongGroupId.trim();
    }

    public String getInlongStreamId() {
        return inlongStreamId;
    }

    public void setInlongStreamId(String inlongStreamId) {
        this.inlongStreamId = inlongStreamId == null ? null : inlongStreamId.trim();
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
        TaskConfigEntity other = (TaskConfigEntity) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
            && (this.getName() == null ? other.getName() == null : this.getName().equals(other.getName()))
            && (this.getType() == null ? other.getType() == null : this.getType().equals(other.getType()))
            && (this.getInlongGroupId() == null ? other.getInlongGroupId() == null : this.getInlongGroupId().equals(other.getInlongGroupId()))
            && (this.getInlongStreamId() == null ? other.getInlongStreamId() == null : this.getInlongStreamId().equals(other.getInlongStreamId()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getName() == null) ? 0 : getName().hashCode());
        result = prime * result + ((getType() == null) ? 0 : getType().hashCode());
        result = prime * result + ((getInlongGroupId() == null) ? 0 : getInlongGroupId().hashCode());
        result = prime * result + ((getInlongStreamId() == null) ? 0 : getInlongStreamId().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", id=").append(id);
        sb.append(", name=").append(name);
        sb.append(", type=").append(type);
        sb.append(", inlongGroupId=").append(inlongGroupId);
        sb.append(", inlongStreamId=").append(inlongStreamId);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}