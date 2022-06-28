package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * Inlong cluster tag entity.
 */
@Data
public class InlongClusterTagEntity implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer id;
    private String clusterTag;
    private String extParams;
    private String inCharges;
    private Integer status;
    private Integer isDeleted;
    private String creator;
    private String modifier;
    private Date createTime;
    private Date modifyTime;

}
