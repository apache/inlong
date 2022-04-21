package org.apache.inlong.sort.protocol.node.format;

public class DebeziumJsonFormatTest extends FormatBaseTest {

    /**
     * Test Serialize and Deserialize of debezium-json
     *
     * @return format
     * @see Format
     */
    @Override
    public Format getFormat() {
        return new DebeziumJsonFormat();
    }
}
