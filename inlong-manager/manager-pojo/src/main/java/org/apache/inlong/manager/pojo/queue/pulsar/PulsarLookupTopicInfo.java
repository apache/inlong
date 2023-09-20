package org.apache.inlong.manager.pojo.queue.pulsar;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PulsarLookupTopicInfo {

    private String brokerUrl;
    private String httpUrl;
    private String nativeUrl;
    private String brokerUrlSsl;
}
