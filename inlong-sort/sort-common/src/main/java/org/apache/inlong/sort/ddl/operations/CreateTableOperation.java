package org.apache.inlong.sort.ddl.operations;

import java.util.List;
import lombok.Data;
import org.apache.inlong.sort.ddl.Column;
import org.apache.inlong.sort.ddl.enums.OperationType;
import org.apache.inlong.sort.ddl.indexes.Index;


@Data
public class CreateTableOperation extends Operation {

    public CreateTableOperation() {
        super(OperationType.CREATE);
    }

    private List<Column> columns;

    private List<Index> indexes;

    private String likeTable;

}
