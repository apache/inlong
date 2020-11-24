# introduction
tubemq-manager is used to manage multiple tubemq cluster. It works with tubemq-web project. 
tubemq-manager provide restful api and tubemq-web use them to provide front-end web pages.
This page is going to introduce how to set up tubemq-manager environment.


# build
```shell script
mvn clean package
```


# distribution
env requirements:
  1. mysql
  2. java(1.8+)

In the dist directory, you can find a installable file called `tubemq-manager-bin.zip`. Unzip it
and add mysql address configuration in `conf/application.properties`

```properties
spring.jpa.hibernate.ddl-auto=update
# configuration for manager
spring.datasource.url=jdbc:mysql://x.x.x.x:3306/tubemanager
spring.datasource.username=xx
spring.datasource.password=xxx
```
Then setup mysql database called `tubemanager`, start this project by this command
```shell script
bin/start-manager.sh
```
