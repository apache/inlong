#### TubeMQ Docker
##### Maven build for docker image
```
mvn -f ../pom.xml clean install -DskipTests -Pdocker
```

##### TubeMQ on Kubernetes
[the Helm Chart for TubeMQ](tubemq-k8s/README.md)