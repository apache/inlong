#### tubemq-manager docker image
TubeMQ manager is available for development and experience.

##### Pull Image
```
docker pull inlong/tubemq-manager:latest
```

##### Start Container
- start MySQL 5.7+
- run Container

```
docker run -d --name manager -p 8089:8089 -e MYSQL_HOST=127.0.0.1 \
-e MYSQL_USER=root -e MYSQL_PASSWD=inlong inlong/tubemq-manager
```
#### Add TubeMQ Cluster to Manager
```
curl --header "Content-Type: application/json" --request POST --data \
'{"masterIp":"master_ip","clusterName":"inlong","masterPort":"8715","masterWebPort":"8080","createUser":"manager","token":"abc"}' \
http://127.0.0.1:8089/v1/cluster?method=add
```