## About WebSite
This is a website console for us to use the [Apache InLong incubator](https://github.com/apache/incubator-inlong).

## Build
```
mvn package -DskipTests -Pdocker -pl inlong-website
```

## Run
```
docker run -d --name website -e MANAGER_API_ADDRESS=127.0.0.1:8083 -p 80:80 inlong/website
```