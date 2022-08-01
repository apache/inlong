package org.apache.inlong.manager.common.pojo.sink.doris;

import lombok.Data;

@Data
public class DorisColumnInfo {

    private String name;

    private String type;

    private String comment;
}
