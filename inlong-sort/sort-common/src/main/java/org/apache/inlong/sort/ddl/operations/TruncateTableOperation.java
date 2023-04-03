package org.apache.inlong.sort.ddl.operations;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.ddl.enums.OperationType;


@EqualsAndHashCode(callSuper = true)
@JsonTypeName("truncateTableOperation")
@JsonInclude(Include.NON_NULL)
@Data
public class TruncateTableOperation extends Operation {

    @JsonCreator
    public TruncateTableOperation() {
        super(OperationType.TRUNCATE);
    }

}
