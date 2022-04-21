package org.apache.inlong.sort.protocol.node.format;

public class CanalJsonFormatTest extends FormatBaseTest {

    /**
     * Test Serialize and Deserialize of canal-json
     *
     * @return format
     * @see Format
     */
    @Override
    public Format getFormat() {
        return new CanalJsonFormat();
    }
}
