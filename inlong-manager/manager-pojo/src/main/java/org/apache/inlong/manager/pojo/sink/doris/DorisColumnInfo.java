package org.apache.inlong.manager.pojo.sink.doris;

import lombok.Data;

/**
 * Doris column info.
 */
@Data
public class DorisColumnInfo {

    private String name;

    private String type;

    private String comment;
}
