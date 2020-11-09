# Apache TubeMQ
[![Build Status](https://travis-ci.org/apache/incubator-tubemq.svg?branch=master)](https://travis-ci.org/apache/incubator-tubemq)
[![CodeCov](https://codecov.io/gh/apache/incubator-tubemq/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-tubemq)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.tubemq/tubemq/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Corg.apache.tubemq)
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://tubemq.apache.org/en-us/docs/download/download.html)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

[Apache TubeMQ](https://tubemq.apache.org)(incubating) is a trillion-records-scale distributed messaging queue (MQ) system, focuses on data transmission and storage under massive data. Compared to many open source MQ projects, TubeMQ has unique advantages in terms of stability, performance, and low cost.

It offers a variety of features:

* Pluggable transport protocols, such as TCP, SSL
* Support big-data and streaming ecosystem integration
* Message retroactivity by time or offset
* Efficient pull and push consumption model
* Flexible distributed scale-out deployment architecture
* Feature-rich administrative dashboard for configuration
* Authentication and authorization


## Contact us

- Mailing lists

    | Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
    |:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
    | [dev@tubemq.apache.org](mailto:dev@tubemq.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@tubemq.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@tubemq.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/tubemq-dev/)   |

- Home page: https://tubemq.apache.org
- Issues: https://issues.apache.org/jira/browse/TubeMQ


## Contributing

We always welcome new contributions, whether for trivial cleanups, new features or other material rewards, more details see [here](https://tubemq.apache.org/en-us/docs/development/how-to-contribute.html).



## Build TubeMQ

### Prerequisites

- Java 1.8
- Maven 3.3+

### Build Distribution Tarball

- Compile and Package:
```bash
mvn clean package -DskipTests
```
- (Optional) Build Using Docker:
```bash
docker run -v REPLACE_WITH_SOURCE_PATH:/tubemq  apachetubemq/tubemq-build clean package -DskipTests
```
- Run Unit Tests:
```bash
mvn test
```
- Build Individual Module:
```bash
mvn clean install
cd module-name (e.g. tubemq-client)
mvn test
```
After the build, please go to `tubemq-server/target`. You can find the
**tubemq-server-[TUBEMQ-VERSION]-bin.tar.gz** file. It is the TubeMQ deployment package, which includes
scripts, configuration files, dependency jars and web GUI code.

### Setting Up Your IDE

If you want to build and debug source code in IDE, go to the project root, and run

```bash
mvn compile
```

This command will generate the Java source files from the `protoc` files, the generated files located in `target/generated-sources`.

(Optional) If you want to use local `protoc` executable, you can change the configuration of `protobuf-maven-plugin` in `tubemq-core/pom.xml` as below

```xml
<configuration>
    <outputDirectory>${project.build.directory}/generated-sources/java</outputDirectory>
    <protocExecutable>/usr/local/bin/protoc</protocExecutable>
</configuration>
```

## Deploy and Start

### Deploy TubeMQ Standalone
Standalone mode starts zookeeper/master/broker services in one docker container:
```
docker run -p 8080:8080 -p 8000:8000 -p 8123:8123 --name tubemq -d apachetubemq/tubemq-all:latest
```
After container is running, you can access ` http://127.0.0.1:8080`, and reference to next `Quick Start` chapter for experience.

**Tips**: Standalone Mode is only available for development and experience, it's not designed for production environment.


### Deploy TubeMQ Cluster
For the detail deployment and configuration of TubeMQ cluster nodes, please refer to the introduction of [Deploy TubeMQ Cluster](https://tubemq.apache.org/en-us/docs/quick_start.html).



## License

© Contributors Licensed under an [Apache-2.0](LICENSE) license.


