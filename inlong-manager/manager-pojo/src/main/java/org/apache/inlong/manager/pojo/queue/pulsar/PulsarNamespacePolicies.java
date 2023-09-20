package org.apache.inlong.manager.pojo.queue.pulsar;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarNamespacePolicies {

    private int message_ttl_in_seconds;
    private PulsarRetentionPolicies retention_policies;
    private PulsarPersistencePolicies persistence;
}
