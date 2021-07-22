#### InLong Agent docker image
InLong Agent is available for development and experience.

##### Pull Image
```
docker pull inlong/agent:latest
```

##### Start Container
```
docker run -d --name agent  -p 8008:8008 \
-e MANAGER_OPENAPI_IP=manager_opeapi_ip -e DATAPROXY_IP=dataproxy_ip \
-e MANAGER_OPENAPI_PORT=8082 -e DATAPROXY_PORT=46801 inlong/agent
```
#### Add A Job
```
curl --location --request POST 'http://localhost:8008/config/job' \
--header 'Content-Type: application/json' \
--data '{
"job": {
"dir": {
"path": "",
"pattern": "/data/inlong-agent/test.log"
},
"trigger": "org.apache.inlong.agent.plugin.trigger.DirectoryTrigger",
"id": 1,
"thread": {
"running": {
"core": "4"
}
},
"name": "fileAgentTest",
"source": "org.apache.inlong.agent.plugin.sources.TextFileSource",
"sink": "org.apache.inlong.agent.plugin.sinks.ProxySink",
"channel": "org.apache.inlong.agent.plugin.channel.MemoryChannel"
},
"proxy": {
"bid": "bid10",
"tid": "bid10"
},
"op": "add"
}'
```

The meaning of each parameter is ：

- job.dir.pattern: Configure the read file path, which can include regular expressions
- job.trigger: Trigger name, the default is DirectoryTrigger, the function is to monitor the files under the folder to generate events
- job.source: The type of data source used, the default is TextFileSource, which reads text files
- job.sink：The type of writer used, the default is ProxySink, which sends messages to the proxy
- proxy.bid: The bid type used when writing proxy
- proxy.tid: The tid type used when writing proxy