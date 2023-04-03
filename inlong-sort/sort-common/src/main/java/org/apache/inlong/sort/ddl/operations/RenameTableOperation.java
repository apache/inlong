package org.apache.inlong.sort.ddl.operations;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.inlong.sort.ddl.enums.OperationType;

@EqualsAndHashCode(callSuper = true)
@JsonTypeName("renameTableOperation")
@JsonInclude(Include.NON_NULL)
@Data
public class RenameTableOperation extends Operation {

        @JsonCreator
        public RenameTableOperation() {
            super(OperationType.RENAME);
        }
}
