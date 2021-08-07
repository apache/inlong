## InLong Manager docker image

InLong Manager is available for development and experience.

### Pull Image

```
docker pull inlong/manager-web:latest
```

### Start Container
~~~~
```
docker run -d --name manager-web -p 8083:8083 \
-e ACTIVE_PROFILE=prod \
-e JDBC_URL='jdbc:mysql:\/\/127.0.0.1:3306\/apache_inlong_manager?serverTimezone=GMT%2b8\&useSSL=false\&allowPublicKeyRetrieval=true\&characterEncoding=UTF-8\&nullCatalogMeansCurrent=true' \
-e USERNAME=xxxxxx \
-e PASSWORD=xxxxxx \
-e TUBE_MANAGER=http://127.0.0.1:8081 \
-e TUBE_MASTER=127.0.0.1:8000,127.0.0.1:8010 \
-e TUBE_CLUSTER_ID=1 \
-e ZK_URL=127.0.0.1:2181 \
-e ZK_ROOT=inlong_hive \
-e SORT_APP_NAME=inlong_hive \
inlong/manager-web:latest
```

### Add A Job

Check if the service started successfully:

[http://localhost:8083/api/inlong/manager/doc.html](http://localhost:8083/api/inlong/manager/doc.html)
