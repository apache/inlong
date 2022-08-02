package org.apache.inlong.manager.common.pojo.sink.doris;

import lombok.Data;


import java.util.List;

/**
 * Doris table info.
 */
@Data
public class DorisTableInfo {

    private String dbName;
    private String tableName;
    private String tableDesc;

    private String partitionBy;
    private String orderBy;
    private String primaryKey;
    private List<DorisColumnInfo> columns;
}
