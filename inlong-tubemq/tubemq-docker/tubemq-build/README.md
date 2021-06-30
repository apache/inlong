### Docker image for building TubeMQ
##### Pull Image
```
docker pull apachetubemq/tubemq-build
```

#### Build TubeMQ
```
docker run -v REPLACE_WITH_SOURCE_PATH:/tubemq  apachetubemq/tubemq-build clean package -DskipTests
```