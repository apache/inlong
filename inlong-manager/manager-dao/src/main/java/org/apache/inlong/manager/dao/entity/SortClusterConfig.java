package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class SortClusterConfig implements Serializable {
    private Integer id;

    private String clusterName;

    private String taskName;

    private static final long serialVersionUID = 1L;

}