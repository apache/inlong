package org.apache.inlong.sort.protocol.node.format;

import org.apache.inlong.sort.SerializeBaseTest;


public class KvFormatTest extends SerializeBaseTest<KvFormat> {

    @Override
    public KvFormat getTestObject() {
        return new KvFormat();
    }
}

