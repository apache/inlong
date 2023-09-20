package org.apache.inlong.manager.pojo.queue.pulsar;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarRetentionPolicies {

    private int retentionTimeInMinutes;
    private long retentionSizeInMB;
}
