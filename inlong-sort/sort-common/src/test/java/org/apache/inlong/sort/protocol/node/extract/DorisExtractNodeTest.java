package org.apache.inlong.sort.protocol.node.extract;

import org.apache.inlong.sort.SerializeBaseTest;
import org.apache.inlong.sort.formats.common.DecimalFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;

import java.util.Arrays;
import java.util.List;

public class DorisExtractNodeTest extends SerializeBaseTest<DorisExtractNode> {
    @Override
    public DorisExtractNode getTestObject() {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("dt", new StringFormatInfo()),
                new FieldInfo("id", new IntFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("price", new DecimalFormatInfo()),
                new FieldInfo("sale", new DoubleFormatInfo())
        );
        return new DorisExtractNode("1", "doris_input", fields,
                null, null, "localhost:8030", "root",
                "000000", "test.test1");
    }
}