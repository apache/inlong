## InLong Manager docker image

InLong Manager is available for development and experience.

### Pull Image

```
docker pull inlong/manager-web:latest
```

### Start Container

```
docker run -d --name manager -p 8083:8083 \
-e ACTIVE_PROFILE=prod \
-e JDBC_URL=127.0.0.1:3306 \
-e USERNAME=root \
-e PASSWORD=inlong \
-e TUBE_MANAGER=127.0.0.1:8089 \
-e TUBE_MASTER=127.0.0.1:8715 \
-e ZK_URL=127.0.0.1:2181 \
-e ZK_ROOT=inlong_hive \
-e SORT_APP_NAME=inlong_hive \
inlong/manager:latest
```

### Add A Job

Check if the service started successfully:

[http://localhost:8083/api/inlong/manager/doc.html](http://localhost:8083/api/inlong/manager/doc.html)
