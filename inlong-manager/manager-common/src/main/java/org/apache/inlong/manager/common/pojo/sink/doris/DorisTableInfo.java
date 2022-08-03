package org.apache.inlong.manager.common.pojo.sink.doris;

import lombok.Data;


import java.util.List;

/**
 * Doris table info.
 */
@Data
public class DorisTableInfo {

    private String tableName;

    private String comment;

    private String primaryKey;

    private String userName;

    private List<DorisColumnInfo> columns;
}
