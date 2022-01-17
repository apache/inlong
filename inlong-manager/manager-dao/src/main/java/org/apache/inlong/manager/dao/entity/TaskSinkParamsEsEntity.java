package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class TaskSinkParamsEsEntity implements Serializable {
    private Integer id;

    private String parentName;

    private String nameService;

    private String userName;

    private String passWord;

    private String esVersion;

    private static final long serialVersionUID = 1L;

}