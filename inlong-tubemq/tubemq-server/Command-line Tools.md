# Overview
TubeMQ provides command-line tools to view and manage the topics as well as the production and consumption of messages.
**tubectl** can be run from TubeMQ's bin directory.
usage:
```
$ bin/tubectl [options] [command] [command options]
```
<<<<<<< HEAD
command:
=======
命令：
>>>>>>> Add unit tests and update documents.

- topic
   - list
   - create
   - update
   - delete
- message
   - produce
   - consume
- cgroup
<<<<<<< HEAD
  - list
  - create
  - delete
=======
>>>>>>> Add unit tests and update documents.
> You can also use --help or -h to get help for the above commands, for example:

```shell
$ bin/tubectl topic -h
```
# Topic
<<<<<<< HEAD
**topic** is used to manage topics in TubeMQ, including adding, deleting, modifying, checking, etc.    
=======
**topic** is used to manage topics in TubeMQ, including adding, deleting, modifying, checking, etc.
>>>>>>> Add unit tests and update documents.
command:

- list
- update
- create
- delete
## list
```shell
$ bin/tubectl topic list
```
options:

| **parameter** | **description** | **default** | **required** |
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
options:

| **parameter** | **description** | **default** | **required** |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | yes |
| -bid, --brokerId | String. Brokers' ID, separated by commas |  | yes |
| -m, --modifyUser | String. Record modifier |  | yes |
| -at, --confModAuthToken | String. Admin api operation authorization cod |  | yes |
| -dp, --deletePolicy | String. File aging strategy |  |  |
| -np, --numPartitions | Int. Number of partitions | 3 |  |
| -uft, --unflushThreshold | Int. Maximum allowed disk unflushing message count | 1000 |  |
| -ufi, --unflushInterval | Int. Maximum allowed disk unflushing interval | 10000 |  |
| -ufd, --unflushDataHold | Int. Maximum allowed disk unflushing data size | 0 |  |
| -nts, --numTopicStores | Int. Number of topic stores | 1 |  |
| -mc, --memCacheMsgCntInK | Int. Maximum allowed memory cache unflushing message count | 10 |  |
| -ms, --memCacheMsgSizeInMB | Int. Maximum allowed memory cache size in MB | 2 |  |
| -mfi, --memCacheFlushIntvl | Int. Maximum allowed disk unflushing data size | 20000 |  |
| -ap, --acceptPublish | Boolean. Enable publishing | true |  |
| -as, --acceptSubscribe | Boolean. Enable subscription | true |  |
| -mms, --maxMsgSizeInMB | Int. Maximum allowed message length, unit MB | 1 |  |
| -md, --modifyDate | String. Record modification date |  |  |

## create
```shell
$ bin/tubectl create
```
options:

| **parameter** | **description** | **default** | **required** |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | yes |
| -bid, --brokerId | String. Brokers' ID, separated by commas |  | yes |
| -c, --createUser | String. Record creator |  | yes |
| -at, --confModAuthToken | String. Admin api operation authorization cod |  | yes |
| -dp, --deletePolicy | String. File aging strategy |  |  |
| -np, --numPartitions | Int. Number of partitions | -1 |  |
| -uft, --unflushThreshold | Int. Maximum allowed disk unflushing message count | -1 |  |
| -ufi, --unflushInterval | Int. Maximum allowed disk unflushing interval | -1 |  |
| -ufd, --unflushDataHold | Int. Maximum allowed disk unflushing data size | 0 |  |
| -nts, --numTopicStores | Int. Number of topic stores | 1 |  |
| -mc, --memCacheMsgCntInK | Int. Maximum allowed memory cache unflushing message count | 10 |  |
| -ms, --memCacheMsgSizeInMB | Int. Maximum allowed memory cache size in MB | 2 |  |
| -mfi, --memCacheFlushIntvl | Int. Maximum allowed disk unflushing data size | 20000 |  |
| -ap, --acceptPublish | Boolean. Enable publishing | true |  |
| -as, --acceptSubscribe | Boolean. Enable subscription | true |  |
| -mms, --maxMsgSizeInMB | Int. Maximum allowed message length, unit MB | 1 |  |
| -cd, --createDate | String. Record creation date |  |  |

## delete
```shell
$ bin/tubectl topic delete
```
options:

| **parameter** | **description** | **default** | **required** |
| --- | --- | --- | --- |
| -o, --deleteOpt | String. Delete options, must in { soft &#124; redo &#124; hard },represents soft deletion, rollback and hard deletion. | soft | yes |
| -n, --topicName | String. Topic name |  | yes |
| -bid, --brokerId | String. Brokers' ID, separated by commas |  | yes |
| -m, --modifyUser | String. Record modifier |  | yes |
| -at, --confModAuthToken | String. Admin api operation authorization code |  | yes |
| -md, --modifyDate | String. Record modification date |  |  |

# Message
<<<<<<< HEAD
**message** is used for message management, including production and consumption.   
=======
**message** is used for message management, including production and consumption.
>>>>>>> Add unit tests and update documents.
command:

- produce
- consume
## produce
```shell
$ bin/tubectl message produce
```
options:

| **parameter** | **description**                                                                                          | **default** | **required** |
| --- |----------------------------------------------------------------------------------------------------------| --- | --- |
| -n, --topicName | String. Topic name                                                                                       |  | yes |
| -ms, --master-servers | String. The master address(es) to connect to. Format is master1_ip:port\[,master2_ip:port\]              |  | yes |
| -m, --mode | String. Produce mode, must in { sync &#124; async }, represents synchronous and asynchronous production. | async                                                                                       |  |
| -t, --msgTotal | Long. The total number of messages to be produced. -1 means unlimited.                                   | -1                                                                                                       |  |

## consume
```shell
$ bin/tubectl message consume
```
options:

| **parameter** | **description**                                                                                                                         | **default** | **required** |
| --- |-----------------------------------------------------------------------------------------------------------------------------------------| --- | --- |
| -n, --topicName | String. Topic name                                                                                                                      |  | yes |
| -g, --groupName | String. Consumer group                                                                                                                  |  | yes |
| -ms, --master-servers | String. The master address(es) to connect to. Format is master1_ip:port\[,master2_ip:port\]                                             |  | yes |
| -m, --mode | String. Consume mode, must in { pull &#124; push &#124; balance }. When the -po parameter is specified, balance mode is used by default. | pull |  |
| -p, --consumePosition | String. Consume position, must in { first &#124; latest &#124; max }                                                                    | latest |  |
| -po, --consumePartitionsAndOffsets | String. Assign consume partitions and their offsets, format is id1:offset1\[,id2:offset2\]\[...\], for example: 0:0,1:0,2:0 |  |  |

# Cgroup
<<<<<<< HEAD
**cgroup** is used for consumer group management. Currently, it supports query, addition, and deletion.    
=======
**cgroup** is used for consumer group management. Currently, it supports query, addition, and deletion.
>>>>>>> Add unit tests and update documents.
command：

- list
- create
- delete
## list
```shell
$ bin/tubectl cgroup list 
```
options:

| **parameter** | **description** | **default** | **required** |
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
options:

| **parameter** | **description** | **default** | **required** |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | yes |
| -g, --groupName | String. Consumer group |  | yes |
| -at, --confModAuthToken | String. Admin api operation authorization code |  | yes |
| -c, --createUser | String. Record creator |  | yes |
| -cd, --createDate | String. Record creation date |  | 
 |

## delete
```shell
$ bin/tubectl cgroup delete
```
options:

| **parameter** | **description** | **default** | **required** |
| --- | --- | --- | --- |
| -n, --topicName | String. Topic name |  | yes |
| -at, --confModAuthToken | String. Admin api operation authorization code |  | yes |
| -g, --groupName | String. Consumer group |  | 
 |

