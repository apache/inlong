## InLong Manager docker image

InLong Manager is available for development and experience.

### Pull Image

```
docker pull inlong/manager-web:latest
```

### Start Container

```
docker run -d --name manager-web -p 8083:8083 inlong/manager-web
```

### Add A Job

Check if the service started successfully:

[http://localhost:8083/api/inlong/manager/doc.html](http://localhost:8083/api/inlong/manager/doc.html)
