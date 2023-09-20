package org.apache.inlong.manager.pojo.queue.pulsar;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarPartitionedInternalStats {

    private Map<String,String> partitions;
    private Map<String,String> metadata;
}
