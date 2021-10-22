package org.apache.inlong.dataproxy.sink.pulsar;

/**
 * description：inlong
 *
 * @Auther: nicobao
 * @Date: 2021/10/21 21:11
 * @Description:
 */
public interface CreatePulsarClientCallBack {

    void handleCreateClientSuccess(String url);

    void handleCreateClientException(String url);
}
