# 总览
提供命令行工具来查看管理TubeMQ的Topic，以及消息的生产与消费。
命令行工具可以在TubeMQ的bin目录运行。
用法
```
$ bin/tubectl [options] [command] [command options]
```
命令：

- topic
   - list
   - create
   - update
   - delete
- message
   - produce
   - consume
- cgroup
> 同时也可以使用--help或者-h获取上述命令的帮助，例如：

```shell
$ bin/tubectl topic -h
```
# Topic
topic命令用于对TubeMQ内的topic进行管理，包括增删改查等等。
命令：

- list
- update
- create
- delete
## list
```shell
$ bin/tubectl topic list
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  |  |
| -sid, --topicStatusId | Int. Topic status ID | 0 |  |
| -bid, --brokerId | String. Brokers' ID, separated by commas |  |  |
| -dp, --deletePolicy | String. File aging strategy |  |  |
| -np, --numPartitions | Int. Number of partitions | 3 |  |
| -nts, --numTopicStores | Int. Number of topic stores | 1 |  |
| -uft, --unflushThreshold | Int. Maximum allowed disk unflushing message count | 1000 |  |
| -ufi, --unflushInterval | Int. Maximum allowed disk unflushing interval | 10000 |  |
| -ufd, --unflushDataHold | Int. Maximum allowed disk unflushing data size | 0 |  |
| -mc, --memCacheMsgCntInK | Int. Maximum allowed memory cache unflushing message count | 10 |  |
| -ms, --memCacheMsgSizeInMB | Int. Maximum allowed memory cache size in MB | 2 |  |
| -mfi, --memCacheFlushIntvl | Int. Maximum allowed disk unflushing data size | 20000 |  |
| -c, --createUser | String. Record creator |  |  |
| -m, --modifyUser | String. Record modifier |  |  |

## update
```shell
$ bin/tubectl topic update
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | 是 |
| -bid, --brokerId | String. Brokers' ID, separated by commas |  | 是 |
| -m, --modifyUser | String. Record modifier |  | 是 |
| -at, --confModAuthToken | String. Admin api operation authorization cod |  | 是 |
| -dp, --deletePolicy | String. File aging strategy |  |  |
| -np, --numPartitions | Int. Number of partitions | 3 |  |
| -uft, --unflushThreshold | Int. Maximum allowed disk unflushing message count | 1000 |  |
| -ufi, --unflushInterval | Int. Maximum allowed disk unflushing interval | 10000 |  |
| -ufd, --unflushDataHold | Int. Maximum allowed disk unflushing data size | 0 |  |
| -nts, --numTopicStores | Int. Number of topic stores | 1 |  |
| -mc, --memCacheMsgCntInK | Int. Maximum allowed memory cache unflushing message count | 10 |  |
| -ms, --memCacheMsgSizeInMB | Int. Maximum allowed memory cache size in MB | 2 |  |
| -mfi, --memCacheFlushIntvl | -mfi, --memCacheFlushIntvl | 20000 |  |
| -ap, --acceptPublish | Boolean. Enable publishing | true |  |
| -as, --acceptSubscribe | Boolean. Enable subscription | true |  |
| -mms, --maxMsgSizeInMB | Int. Maximum allowed message length, unit MB | 1 |  |
| -md, --modifyDate | String. Record modification date |  |  |

## create
```shell
$ bin/tubectl create
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | 是 |
| -bid, --brokerId | String. Brokers' ID, separated by commas |  | 是 |
| -c, --createUser | String. Record creator |  | 是 |
| -at, --confModAuthToken | String. Admin api operation authorization cod |  | 是 |
| -dp, --deletePolicy | String. File aging strategy |  |  |
| -np, --numPartitions | Int. Number of partitions | -1 |  |
| -uft, --unflushThreshold | Int. Maximum allowed disk unflushing message count | -1 |  |
| -ufi, --unflushInterval | Int. Maximum allowed disk unflushing interval | -1 |  |
| -ufd, --unflushDataHold | Int. Maximum allowed disk unflushing data size | 0 |  |
| -nts, --numTopicStores | Int. Number of topic stores | 1 |  |
| -mc, --memCacheMsgCntInK | Int. Maximum allowed memory cache unflushing message count | 10 |  |
| -ms, --memCacheMsgSizeInMB | Int. Maximum allowed memory cache size in MB | 2 |  |
| -mfi, --memCacheFlushIntvl | -mfi, --memCacheFlushIntvl | 20000 |  |
| -ap, --acceptPublish | Boolean. Enable publishing | true |  |
| -as, --acceptSubscribe | Boolean. Enable subscription | true |  |
| -mms, --maxMsgSizeInMB | Int. Maximum allowed message length, unit MB | 1 |  |
| -cd, --createDate | String. Record creation date |  |  |

## delete
```shell
$ bin/tubectl topic delete
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -o, --deleteOpt | Delete options, must in { soft &#124; redo &#124; hard } | soft | 是 |
| -n, --topicName | String. Topic name |  | 是 |
| -bid, --brokerId | String. Brokers' ID, separated by commas |  | 是 |
| -m, --modifyUser | String. Record modifier |  | 是 |
| -at, --confModAuthToken | String. Admin api operation authorization code |  | 是 |
| -md, --modifyDate | String. Record modification date |  |  |

# Message
topic命令用于消息管理，包括生产和消费。
命令：

- produce
- consume
## produce
```shell
$ bin/tubectl message produce
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | 是 |
| -ms, --master-servers | String. The master address(es) to connect to. 
Format is master1_ip:port[,master2_ip:port] |  | 是 |
| -m, --mode | String. Produce mode, must in { sync &#124; async } | async |  |

## consume
```shell
$ bin/tubectl message consume
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | 是 |
| -g, --groupName | String. Consumer group |  | 是 |
| -ms, --master-servers | String. The master address(es) to connect to. 
Format is master1_ip:port[,master2_ip:port] |  | 是 |
| -m, --mode | String. Consume mode, must in { pull &#124; push &#124; balance }
当指定了-p参数时，默认使用balance模式 | pull |  |
| -p, --consumePosition | String. Consume position, must in { first &#124; latest &#124; max } | latest |  |
| -po, 
--consumePartitionsAndOffsets | String. Consume partitions and their offsets, 
format is id1:offset1[,id2:offset2][...], 
for example: 0:0,1:0,2:0 |  |  |

# Cgroup
cgroup命令用于消费者组管理，目前支持查询、增加和删除。
命令：

- list
- create
- delete
## list
```shell
$ bin/tubectl cgroup list 
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | 
 |
| -g, --groupName | String. Consumer group |  | 
 |
| -c, --createUser | String. Record creator |  |  |

## create
```shell
$ bin/tubectl cgroup create
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | 是 |
| -g, --groupName | String. Consumer group |  | 是 |
| -at, --confModAuthToken | String. Admin api operation authorization code |  | 是 |
| -c, --createUser | String. Record creator |  | 是 |
| -cd, --createDate | String. Record creation date |  | 
 |

## delete
```shell
$ bin/tubectl cgroup delete
```
选项：

| 参数 | 描述 | 默认值 | 必需 |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | 是 |
| -at, --confModAuthToken | String. Admin api operation authorization code |  | 是 |
| -g, --groupName | String. Consumer group |  | 
 |

