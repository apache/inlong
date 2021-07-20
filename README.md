<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->


# Apache InLong
[![Build Status](https://travis-ci.org/apache/incubator-inlong.svg?branch=master)](https://travis-ci.org/apache/incubator-inlong)
[![CodeCov](https://codecov.io/gh/apache/incubator-inlong/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-inlong)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.inlong/inlong/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Corg.apache.inlong)
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://inlong.apache.org/en-us/docs/download/download.html)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)


- [What is Apache InLong?](#what-is-apache-inlong)
- [Features](#features)
- [When should I use InLong?](#when-should-i-use-inlong)
- [Building InLong](#building-inlong)
- [Deploying InLong](#deploying-inlong)
- [Join the Community](#join-the-community)
- [Documentation](#documentation)
- [License](#license)

# What is Apache InLong?
[Apache InLong](https://inlong.apache.org)(incubating) is a one-stop data streaming platform that provides automatic, secure, distributed, and efficient data publishing and subscription capabilities. This platform helps you easily build stream-based data applications.

InLong was originally built at Tencent, has served online businesses for more than 8 years, to support massive data (data scale of more than 40 trillion pieces of data per day) reporting services in big data scenarios. The entire platform has integrated 5 modules:  Ingestion, Convergence, Caching, Sorting, and Management, so that the business only needs to provide data sources, data service quality, data landing clusters and data landing formats, that is, the data can be continuously pushed from the source to the target cluster greatly meets the data reporting service requirements in the business big data scenario.

For getting more information, please visit our project documentation at https://inlong.apache.org/en-us/ .
<img src="https://github.com/apache/incubator-inlong-website/blob/master/img/inlong_architecture.png" align="center" alt="Apache InLong"/>


## Features
Apache InLong offers a variety of features:
* **Ease of Use**: a SaaS-based service platform, you can easily and quickly report, transfer, and distribute data by publishing and subscribing to data based on topics.
* **Stability & Reliability**: derived from the actual online production environment, it delivers high-performance processing capabilities for 10 trillion-level data streams and highly reliable services for 100 billion-level data streams.
* **Comprehensive Features**: supports various types of data access methods and can be integrated with different types of Message Queue (MQ) services, it also provides real-time data extract, transform, and load (ETL) and sorting capabilities based on rules, allows you to plug features to extend system capabilities.
* **Service Integration**: provides unified system monitoring and alert services, it provides fine-grained metrics to facilitate data visualization, you can view the running status of queues and topic-based data statistics in a unified data metric platform, configure the alert service based on your business requirements so that users can be alerted when errors occur.
* **Scalability**: adopts a pluggable architecture that allows you to plug modules into the system based on specific protocols, so you can replace components and add features based on your business requirements


## When should I use InLong?
InLong is based on MQ and aims to provide a one-stop, practice-tested module pluggable data stream access service platform，based on this system, users can easily build stream-based data applications. It is suitable for environments that need to quickly build a data reporting platform, as well as an ultra-large-scale data reporting environment that InLong is very suitable for, and an environment that needs to automatically sort and land the reported data.

InLong is only a one-stop data reporting pipeline platform. It cannot be used as a persistent data storage, nor does it support complex business logic processing on data streams.

## Build InLong
More detailed instructions can be found at [Quick Start](https://inlong.apache.org/en-us/docs/quick_start.html) section in the documentation.

Requirements:
- Java [JDK 8](https://adoptopenjdk.net/?variant=openjdk8)
- Maven 3.6.1+

Compile and install:
```
$ mvn clean install -DskipTests
```
(Optional) Compile using docker image:
```
$ docker pull maven:3.6-openjdk-8
$ docker run -v `pwd`:/inlong  -w /inlong maven:3.6-openjdk-8 mvn clean install -DskipTests
```
after compile successfully, you could find distribution file at `inlong-distribution/target`.

## Deploy InLong
InLong integrates a complete component chain for data reporting in big data scenarios, and not support automatic installation of modules now, so we need to choose manually  to install all or some modules according to actual needs. Please refer to [Quick Start](https://inlong.apache.org/en-us/docs/quick_start.html) in our project documentation.

## Contribute InLong
- Report any issue on [Jira](https://issues.apache.org/jira/browse/InLong)
- Code pull request according to [How to contribute](https://inlong.apache.org/en-us/docs/development/how-to-contribute.html).

## Contact Us
- Join Apache InLong mailing lists:
    | Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
    |:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
    | [dev@inlong.apache.org](mailto:dev@inlong.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@inlong.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@inlong.apache.org)   | [Archives](http://mail-archives.apache.org/mod_mbox/inlong-dev/)   |
- Ask questions on [Apache InLong Slack](https://the-asf.slack.com/archives/C01QAG6U00L)

## Documentation
- Home page: https://inlong.apache.org/en-us/
- Issues: https://issues.apache.org/jira/browse/InLong

## License
© Contributors Licensed under an [Apache-2.0](LICENSE) license.


