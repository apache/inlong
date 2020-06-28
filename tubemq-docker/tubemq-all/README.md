#### tubemq-all docker image
TubeMQ standalone is available for development and experience.

##### Pull Image
```
docker pull apachetubemq/tubemq-all:latest
```

##### Start Standalone Container
```
docker run -p 8080:8080 -p 8000:8000 --name tubemq -d apachetubemq/tubemq-all:latest
```
this command will start zookeeper/master/broker service in one container.
#### Add Topic
If the container is running, you can access http://127.0.0.1:8080 to see the web GUI, and you can reference to the **Add Topic** part of [user guide](https://tubemq.apache.org/en-us/docs/tubemq_user_guide.html)