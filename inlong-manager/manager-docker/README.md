## InLong Manager docker image

InLong Manager is available for development and experience.

### Pull Image

```
docker pull inlong/manager:latest
```

### Start Container

```
docker run -d --name manager-web -p 8083:8083 \
-e MANAGER_OPENAPI_PORT=8082 -e DATAPROXY_PORT=46801 inlong/manager-web
```

## Add A Job

```
curl --location --request POST 'http://localhost:8008/config/job' \
--header 'Content-Type: application/json' \
--data '{
"job": {
"dir": {
"path": "",
"pattern": "/data/inlong-agent/test.log"
}'
```

The meaning of each parameter is ：

- job.dir.pattern: Configure the read file path, which can include regular expressions
