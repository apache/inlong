package org.apache.inlong.dataproxy.sink;

import com.google.common.base.Charsets;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.testng.Assert;

/**
 * description： send to pulsar test
 * @Auther: nicobao
 * @Date: 2021/10/21 16:55
 * @Description:
 */
public class TestPulsarSink {
    private static final Logger logger = LoggerFactory
            .getLogger(TestPulsarSink.class);
    private static final String hostname = "127.0.0.1";
    private static final Integer port = 41414;
    private String zkStr = "10.196.129.19:2181,10.196.129.20:2181,10.196.129.21:2181";
    private String zkRoot = "/meta";
    private int batchSize = 1;

    private PulsarSink sink;
    private Channel channel;

    @BeforeClass
    public void setUp() {
        sink = new PulsarSink();
        channel = new MemoryChannel();

        Context context = new Context();

        context.put("type", "org.apache.inlong.dataproxy.sink.PulsarSink");
        context.put("pulsar_server_url_list", "pulsar://127.0.0.1:6650");

        sink.setChannel(channel);

        Configurables.configure(sink, context);
        Configurables.configure(channel, context);
    }

    @Test
    public void testProcess() throws InterruptedException, EventDeliveryException,
            InstantiationException, IllegalAccessException {
        setUp();
        Event event = EventBuilder.withBody("test event 1", Charsets.UTF_8);
        sink.start();
        Assert.assertTrue(LifecycleController.waitForOneOf(sink,
                LifecycleState.START_OR_ERROR, 5000));

        Transaction transaction = channel.getTransaction();

        transaction.begin();
        for (int i = 0; i < 10; i++) {
            channel.put(event);
        }
        transaction.commit();
        transaction.close();

        for (int i = 0; i < 5; i++) {
            Sink.Status status = sink.process();
            Assert.assertEquals(Sink.Status.READY, status);
        }

        sink.stop();
        Assert.assertTrue(LifecycleController.waitForOneOf(sink,
                LifecycleState.STOP_OR_ERROR, 5000));
    }

}
