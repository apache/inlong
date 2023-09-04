# 1 总览
Tubectl是一个TubeMQ命令行工具，用来支持TubeMQ内topic的增删改查、消息的生产与发送、消费者组管理等等。
# 2 调用关系
## 2.1 bin脚本
bin目录下的tubectl脚本用于设置环境变量，并调用CommandToolMain程序。
```shell
$ source $BASE_DIR/bin/env.sh
$ JAVA $TOOLS_ARGS org.apache.inlong.tubemq.server.tools.cli.CommandToolMain $@
```
## 2.2 CliWebapiAdmin
用于执行http请求，支持传入Map<String, Object>作为请求参数，主要由processParams方法执行请求，该方法以String[] args传入请求方法名。调用示例如下：
```java
String[] requestMethod = new String[]{"--method", ""};
Map<String, Object> requestParams = new HashMap<>();

requestMethod[1] = "admin_query_topic_info";
CliWebapiAdmin cliWebapiAdmin = new CliWebapiAdmin(requestParams);
cliWebapiAdmin.processParams(requestMethod);
```
## 2.3 CommandToolMain
在org.apache.inlong.tubemq.server.tools.cli包下。使用了Jcommander命令行解析框架。该程序是命令行解析入口，根据解析到的参数，分别调用topic、message、cgroup子命令。
```java
CommandToolMain() {
    jcommander = new JCommander();
    jcommander.setProgramName("tubectl");
    jcommander.addObject(this);
    jcommander.addCommand("topic", new TopicCommand());
    jcommander.addCommand("message", new MessageCommand());
    jcommander.addCommand("cgroup", new CgroupCommand());
}
```
### 2.2.1 TopicCommand
负责处理tubectl topic子命令。
```java
public TopicCommand() {
    super("topic");
    jcommander.addCommand("list", new TopicList());
    jcommander.addCommand("update", new TopicUpdate());
    jcommander.addCommand("create", new TopicCreate());
    jcommander.addCommand("delete", new TopicDelete());
    
    @Parameters(commandDescription = "Topic List")
    private static class TopicList extends AbstractCommandRunner {}

    @Parameters(commandDescription = "Topic Update")
    private static class TopicUpdate extends AbstractCommandRunner {}

    @Parameters(commandDescription = "Topic Create")
    private static class TopicCreate extends AbstractCommandRunner {}

    @Parameters(commandDescription = "Topic Delete")
    private static class TopicDelete extends AbstractCommandRunner {}
}
```
TopicCommand类根据解析到的参数，分别调用对应类的run方法。以list命令为例：
```java
void run() {
    try {
        requestMethod[1] = "admin_query_topic_info";
        requestParams.clear();
        requestParams.put(WebFieldDef.NUMTOPICSTORES.name, numTopicStores);
        //  ...
        requestParams.put(WebFieldDef.UNFLUSHTHRESHOLD.name, unflushThreshold);
        cliWebapiAdmin.processParams(requestMethod);
    } catch (Exception e) {
        System.out.println(e.getMessage());
    }
}
```
指定Web请求方法以及请求参数，并构造CliWebapiAdmin，调用其processParams方法得到输出。
### 2.2.2 MessageCommand
负责处理tubectl message子命令，生产和消费消息。生产者和消费者使用TubeSingleSessionFactory建造。
```java
public MessageCommand() {
    super("message");
    jcommander.addCommand("produce", new MessageProduce());
    jcommander.addCommand("consume", new MessageConsume());
    
    @Parameters(commandDescription = "Produce message")
    private static class MessageProduce extends AbstractCommandRunner{}
    
    @Parameters(commandDescription = "Consume message")
    private static class MessageConsume extends AbstractCommandRunner{}
}
```
**生产消息**
支持的生产模式：

1. sync模式，即同步生产
```java
private void syncProduce(Message message) throws TubeClientException, InterruptedException {
    MessageSentResult result = messageProducer.sendMessage(message);
    if (!result.isSuccess()) {
        System.out.println("sync send message failed : " + result.getErrMsg());
    } else {
        msgCount.getAndIncrement();
    }
}
```

2. async模式，即异步生产
```java
private void asyncProduce(Message message) throws TubeClientException, InterruptedException {
    messageProducer.sendMessage(message, new MessageSentCallback() {

        @Override
        public void onMessageSent(MessageSentResult result) {
            if (!result.isSuccess()) {
                System.out.println("async send message failed : " + result.getErrMsg());
            } else {
                msgCount.getAndIncrement();
            }
        }

        @Override
        public void onException(Throwable e) {
            System.out.println("async send message error : " + e);
        }
    });
}
```
**消费消息**
支持的生产模式：

1. pull模式
```java
private void pullConsumer(MessageSessionFactory messageSessionFactory, ConsumerConfig consumerConfig)
        throws TubeClientException {
    messagePullConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
    messagePullConsumer.subscribe(topicName, null);
    messagePullConsumer.completeSubscribe();
    while (!messagePullConsumer.isPartitionsReady(1000)) {
        ThreadUtils.sleep(1000);
    }
    // System.out.println(messagePullConsumer.getCurConsumedPartitions());
    while (true) {
        ConsumerResult result = messagePullConsumer.getMessage();
        if (result.isSuccess()) {
            List<Message> messageList = result.getMessageList();
            for (Message message : messageList) {
                System.out.println(new String(message.getData()));
                msgCount.getAndIncrement();
            }
            messagePullConsumer.confirmConsume(result.getConfirmContext(), true);
        }
    }
}
```

2. push模式
```java
private void pushConsumer(MessageSessionFactory messageSessionFactory, ConsumerConfig consumerConfig)
        throws TubeClientException, InterruptedException {
    messagePushConsumer = messageSessionFactory.createPushConsumer(consumerConfig);
    messagePushConsumer.subscribe(topicName, null, new MessageListener() {

        @Override
        public void receiveMessages(PeerInfo peerInfo, List<Message> messages) throws InterruptedException {
            for (Message message : messages) {
                System.out.println(new String(message.getData()));
                msgCount.getAndIncrement();
            }
        }

        @Override
        public Executor getExecutor() {
            return null;
        }

        @Override
        public void stop() {
        }
    });
    messagePushConsumer.completeSubscribe();
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(10, TimeUnit.MINUTES);
}
```

3. clientBanlance模式

详见[客户端分区分配](https://inlong.apache.org/zh-CN/docs/next/modules/tubemq/client_partition_assign_introduction)
```java
private void balanceConsumer(MessageSessionFactory messageSessionFactory, ConsumerConfig consumerConfig)
        throws TubeClientException {
    clientBalanceConsumer = messageSessionFactory.createBalanceConsumer(consumerConfig);
    ProcessResult procResult = new ProcessResult();
    QueryMetaResult qryResult = new QueryMetaResult();
    final Map<String, TreeSet<String>> topicAndFiltersMap =
            MixedUtils.parseTopicParam(topicName);
    if (!clientBalanceConsumer.start(topicAndFiltersMap, -1, 0, procResult)) {
        System.out.println("Initial balance consumer failure, errcode is " + procResult.getErrCode()
                + " errMsg is " + procResult.getErrMsg());
        return;
    }
    clientBalanceConsumer.getPartitionMetaInfo(qryResult);
    Map<String, Boolean> partMetaInfoMap = qryResult.getPartStatusMap();
    if (partMetaInfoMap != null && !partMetaInfoMap.isEmpty()) {
        Set<String> configuredTopicPartitions = partMetaInfoMap.keySet();
        Map<Long, Long> assignedPartitionsAndOffsets = new HashMap<>();
        for (String str : consumePartitionsAndOffsets.split(",")) {
            String[] splits = str.split(":");
            assignedPartitionsAndOffsets.put(Long.parseLong(splits[0]), Long.parseLong(splits[1]));
        }
        Set<Long> assignedPartitionIds = assignedPartitionsAndOffsets.keySet();
        Set<String> assignedPartitions = new HashSet<>();
        for (String partKey : configuredTopicPartitions) {
            long parId = Long.parseLong(partKey.split(":")[2]);
            if (partMetaInfoMap.get(partKey) && assignedPartitionIds.contains(parId)) {
                assignedPartitions.add(partKey);
                Long boostrapOffset = assignedPartitionsAndOffsets.get(parId);
                // System.out.println(boostrapOffset);
                if (!clientBalanceConsumer.connect2Partition(partKey,
                        boostrapOffset == null ? -1L : boostrapOffset, procResult))
                    System.out.println("connect2Partition failed.");
            }
        }

        ConsumeResult csmResult = new ConsumeResult();
        ConfirmResult cfmResult = new ConfirmResult();
        // System.out.println("Before partition ready...");
        // wait partition status ready
        while (!clientBalanceConsumer.isPartitionsReady(1000)) {
            ThreadUtils.sleep(1000);
        }
        // System.out.println(clientBalanceConsumer.getCurPartitionOffsetInfos());
        // consume messages
        while (true) {
            // get messages
            if (clientBalanceConsumer.getMessage(csmResult)) {
                List<Message> messageList = csmResult.getMessageList();
                for (Message message : messageList) {
                    System.out.println(new String(message.getData()));
                    msgCount.getAndIncrement();
                }
                // confirm messages to server
                clientBalanceConsumer.confirmConsume(csmResult.getConfirmContext(), true, cfmResult);
                // System.out.println(clientBalanceConsumer.getCurPartitionOffsetInfos());
            }
        }

    } else {
        System.out.println("No partitions of the topic are available now.");
    }

}
```
### 2.2.3 CgroupCommand
```java
public CgroupCommand() {
    super("cgroup");

    jcommander.addCommand("list", new CgroupList());
    jcommander.addCommand("create", new CgroupCreate());
    jcommander.addCommand("delete", new CgroupDelete());

    @Parameters(commandDescription = "Consumer group List")
    private static class CgroupList extends AbstractCommandRunner{}

	@Parameters(commandDescription = "Consumer group Create")
    private static class CgroupCreate extends AbstractCommandRunner{}

    @Parameters(commandDescription = "Consumer group Delete")
    private static class CgroupDelete extends AbstractCommandRunner{}
}
```

