package org.apache.inlong.sort.ddl.indexes;

import java.util.List;
import lombok.Data;
import org.apache.inlong.sort.ddl.enums.IndexType;

/**
 * @Author pengzirui
 * @Date 2023/3/22 7:51 PM
 * @Version 1.0
 */
@Data
public class Index {

    private IndexType indexType;

    private String indexName;

    private List<String> indexColumns;

}
