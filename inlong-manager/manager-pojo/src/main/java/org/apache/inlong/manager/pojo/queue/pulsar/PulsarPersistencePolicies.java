package org.apache.inlong.manager.pojo.queue.pulsar;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarPersistencePolicies {

    private int bookkeeperEnsemble;
    private int bookkeeperWriteQuorum;
    private int bookkeeperAckQuorum;
    private double managedLedgerMaxMarkDeleteRate;
}
