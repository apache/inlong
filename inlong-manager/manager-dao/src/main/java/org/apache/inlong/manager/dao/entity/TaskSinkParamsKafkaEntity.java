package org.apache.inlong.manager.dao.entity;

import java.io.Serializable;

public class TaskSinkParamsKafkaEntity implements Serializable {
    private Integer id;

    private String parentName;

    private String zkList;

    private String brokerList;

    private static final long serialVersionUID = 1L;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getParentName() {
        return parentName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName == null ? null : parentName.trim();
    }

    public String getZkList() {
        return zkList;
    }

    public void setZkList(String zkList) {
        this.zkList = zkList == null ? null : zkList.trim();
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList == null ? null : brokerList.trim();
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
        TaskSinkParamsKafkaEntity other = (TaskSinkParamsKafkaEntity) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
            && (this.getParentName() == null ? other.getParentName() == null : this.getParentName().equals(other.getParentName()))
            && (this.getZkList() == null ? other.getZkList() == null : this.getZkList().equals(other.getZkList()))
            && (this.getBrokerList() == null ? other.getBrokerList() == null : this.getBrokerList().equals(other.getBrokerList()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getParentName() == null) ? 0 : getParentName().hashCode());
        result = prime * result + ((getZkList() == null) ? 0 : getZkList().hashCode());
        result = prime * result + ((getBrokerList() == null) ? 0 : getBrokerList().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", id=").append(id);
        sb.append(", parentName=").append(parentName);
        sb.append(", zkList=").append(zkList);
        sb.append(", brokerList=").append(brokerList);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}