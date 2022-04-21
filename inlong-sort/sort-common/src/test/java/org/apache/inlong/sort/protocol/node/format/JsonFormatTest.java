package org.apache.inlong.sort.protocol.node.format;

public class JsonFormatTest extends FormatBaseTest {

    /**
     * Test Serialize and Deserialize of json
     *
     * @return format
     * @see Format
     */
    @Override
    public Format getFormat() {
        return new JsonFormat();
    }
}
