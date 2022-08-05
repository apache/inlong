package org.apache.inlong.sort.protocol.node.load;

import org.apache.inlong.sort.SerializeBaseTest;
import org.apache.inlong.sort.formats.common.DecimalFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.enums.FilterStrategy;
import org.apache.inlong.sort.protocol.transformation.FieldRelation;
import org.apache.inlong.sort.protocol.transformation.FilterFunction;

import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class DorisLoadNodeTest extends SerializeBaseTest<DorisLoadNode> {
    @Override
    public DorisLoadNode getTestObject() {
        List<FieldInfo> fields = Arrays.asList(
                new FieldInfo("dt", new StringFormatInfo()),
                new FieldInfo("id", new IntFormatInfo()),
                new FieldInfo("name", new StringFormatInfo()),
                new FieldInfo("age", new IntFormatInfo()),
                new FieldInfo("price", new DecimalFormatInfo()),
                new FieldInfo("sale", new DoubleFormatInfo())
        );

        List<FieldRelation> fieldRelations = Arrays
                .asList(new FieldRelation(new FieldInfo("dt", new StringFormatInfo()),
                                new FieldInfo("dt", new StringFormatInfo())),
                        new FieldRelation(new FieldInfo("id", new IntFormatInfo()),
                                new FieldInfo("id", new IntFormatInfo())),
                        new FieldRelation(new FieldInfo("name", new StringFormatInfo()),
                                new FieldInfo("name", new StringFormatInfo())),
                        new FieldRelation(new FieldInfo("age", new IntFormatInfo()),
                                new FieldInfo("age", new IntFormatInfo())),
                        new FieldRelation(new FieldInfo("price", new DecimalFormatInfo()),
                                new FieldInfo("price", new DecimalFormatInfo())),
                        new FieldRelation(new FieldInfo("sale", new DoubleFormatInfo()),
                                new FieldInfo("sale", new DoubleFormatInfo()))
                );

        List<FilterFunction> filters = new ArrayList<>();
        FilterStrategy filterStrategy = null;
        Map<String, String> map = new HashMap<>();
        return new DorisLoadNode("2", "doris_output", fields, fieldRelations,
                filters, filterStrategy, 1, map,
                "localhost:8030", "root",
                "000000", "test.test2", null);
    }
}
