License
=======

Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file
distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.

# introduction

tube-admin is used to manage multiple tubemq cluster. It works with tubemq-web project. tube-admin provide restful api
and tubemq-web use them to provide front-end web pages. This page is going to introduce how to set up tubemq-admin
environment.

# build

```shell script
mvn clean package
```

# distribution

env requirements:

1. mysql
2. java(1.8+)

In the dist directory, you can find a installable file called `tubemq-manager-bin.zip`. Unzip it and add mysql address
configuration in `conf/application.properties`

```properties
spring.jpa.hibernate.ddl-auto=update
# configuration for manager
spring.datasource.url=jdbc:mysql://x.x.x.x:3306/tubeAdmin
spring.datasource.username=xx
spring.datasource.password=xxx
```

Then setup mysql database called `tubeadmin`, start this project by this command

```shell script
bin/start-admin.sh
```
