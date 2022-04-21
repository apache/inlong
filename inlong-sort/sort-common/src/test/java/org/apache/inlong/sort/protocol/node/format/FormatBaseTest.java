package org.apache.inlong.sort.protocol.node.format;

import com.google.common.base.Preconditions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class FormatBaseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Format format;

    /**
     * init format object
     */
    @Before
    public void before() {
        this.format = Preconditions.checkNotNull(getFormat());
    }

    public abstract Format getFormat();

    /**
     * Test Serialize and Deserialize
     */
    @Test
    public void testSerializeAndDeserialize() throws JsonProcessingException {
        String jsonStr = objectMapper.writeValueAsString(format);
        Format deserializeFormat = objectMapper.readValue(jsonStr, Format.class);
        assertEquals(format, deserializeFormat);
    }

}
