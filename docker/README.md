## Docker For InLong

Requirements:
- [Docker](https://docs.docker.com/engine/install/) 19.03.1+

### Build Images
```
mvn clean package -DskipTests -Pdocker
```

### Run All Modules
- [docker-compose](docker-compose/README.md)
