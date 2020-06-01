### Docker image for building TubeMQ
#### pull
- for all contributors
```
docker pull tdbank/tubemq-build
```
- for contributors who worked for tencent
```
docker pull csighub.tencentyun.com/tdbank/tubemq-build:latest
```

#### build
```
docker build  -t tdbank/tubemq-build ./
```

#### run
```
docker run -v REPLACE_WITH_SOURCE_PATH:/tubemq  csighub.tencentyun.com/tdbank/tubemq-build clean package -DskipTests
```