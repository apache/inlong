package org.apache.inlong.dataproxy.sink.pulsar;

import org.apache.inlong.dataproxy.sink.EventStat;

/**
 * description：inlong
 *
 * @Auther: nicobao
 * @Date: 2021/10/21 21:11
 * @Description:
 */
public interface SendMessageCallBack {

    void handleMessageSendSuccess(Object msgId, EventStat es);

    void handleMessageSendException(EventStat es, Object exception);
}
