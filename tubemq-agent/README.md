# project introduction

tubemq agent is used to collect data from local and sent them to MQ. It is implemented based on 
source/channel/sink architecture, and also, it has fine-grained of controlling jobs which is
easy for users to stop/delete/add collection jobs. It supports reading from local files/SQL data/binlog data. 
Cron expressions and directory watchers are supported in tubemq agent.

### build

```shell script
mvn clean package -DskipTests
```

### package

```shell script
mvn clean package assembly:single -DskipTests 
```
you can find tar.gz in build directory.

### run test cases

```shell script
mvn clean package
```

## contribution

please read it [CONTRIBUTING.md](https://tubemq.apache.org/en-us/)


