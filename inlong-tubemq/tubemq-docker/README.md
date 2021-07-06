#### InLong TubeMQ Docker
##### Maven build for docker image
```
mvn -f ../pom.xml clean install -DskipTests -Pdocker
```

##### InLong TubeMQ on Kubernetes
[the Helm Chart for TubeMQ](tubemq-k8s/README.md)