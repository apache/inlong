package org.apache.inlong.agent.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.agent.pojo.DebeziumOffset;

import java.io.IOException;

/**
 * @description
 * @date: 2022/5/9
 */
public class DebeziumOffsetSerializer {

    public static final DebeziumOffsetSerializer INSTANCE = new DebeziumOffsetSerializer();

    public byte[] serialize(DebeziumOffset debeziumOffset) throws IOException {
        // we currently use JSON serialization for simplification, as the state is very small.
        // we can improve this in the future if needed
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsBytes(debeziumOffset);
    }

    public DebeziumOffset deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(bytes, DebeziumOffset.class);
    }
}
