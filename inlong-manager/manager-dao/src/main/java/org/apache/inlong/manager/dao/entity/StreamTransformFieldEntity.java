package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.io.Serializable;

@Data
public class StreamTransformFieldEntity implements Serializable {

    private Integer id;

    private String inlongGroupId;

    private String inlongStreamId;

    private Integer transformId;

    private String transformType;

    private String fieldName;

    private String fieldValue;

    private String preExpression;

    private String fieldType;

    private String fieldComment;

    private Short isMetaField;

    private String fieldFormat;

    private Short rankNum;

    private Integer isDeleted;
}