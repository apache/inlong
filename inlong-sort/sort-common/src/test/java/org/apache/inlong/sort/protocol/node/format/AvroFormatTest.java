package org.apache.inlong.sort.protocol.node.format;

public class AvroFormatTest extends FormatBaseTest {

    /**
     * Test Serialize and Deserialize of avro
     *
     * @return format
     * @see Format
     */
    @Override
    public Format getFormat() {
        return new AvroFormat();
    }
}
