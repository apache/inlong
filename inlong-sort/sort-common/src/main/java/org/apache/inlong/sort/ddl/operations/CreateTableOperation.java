package org.apache.inlong.sort.ddl.operations;

import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.ddl.Column;
import org.apache.inlong.sort.ddl.enums.OperationType;
import org.apache.inlong.sort.ddl.indexes.Index;


@EqualsAndHashCode(callSuper = true)
@JsonTypeName("createTableOperation")
@JsonInclude(Include.NON_NULL)
@Data
public class CreateTableOperation extends Operation {

    public CreateTableOperation() {
        super(OperationType.CREATE);
    }

    @JsonProperty("columns")
    private List<Column> columns;

    @JsonProperty("indexes")
    private List<Index> indexes;

    @JsonProperty("likeTable")
    private String likeTable;

}
