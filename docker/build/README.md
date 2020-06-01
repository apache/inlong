### Docker image for building TubeMQ
#### pull
- for all contributors
```
docker pull apachetubemq/tubemq-build
```

#### build
```
docker build  -t apachetubemq/tubemq-build ./
```

#### run
```
docker run -v REPLACE_WITH_SOURCE_PATH:/tubemq  apachetubemq/tubemq-build clean package -DskipTests
```