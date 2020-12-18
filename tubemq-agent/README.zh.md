# 项目说明

tdbank-agent 数据采集工具，支持多种数据源，提供高效、可靠、易于维护的数据同步服务

## 开始工作
项目设计文档请参考: [tdbank-agent架构设计](https://iwiki.oa.tencent.com/pages/viewpage.action?pageId=144697373)

### 环境准备

本项目使用java开发，maven管理，配置请参考: 
- [maven 腾讯软件源](https://iwiki.oa.tencent.com/pages/viewpage.action?pageId=144697373)
- [JDK 1.8及以上 ](http://www.oracle.com/technetwork/cn/java/javase/downloads/index.html)
- [Apache Maven 3.x](https://maven.apache.org/download.cgi)


### 编译

maven项目编译命令

```shell script
mvn clean package -DskipTests
```

### 打包

项目打包成可安装的tgz文件，执行命令如下

```shell script
mvn clean package assembly:single -DskipTests 
```
在项目下的target里面可以找到tgz安装包

### test case运行

执行test case命令如下

```shell script
mvn clean package
```


## 贡献

请阅读 [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2), 欢迎参与项目讨论和开发.

## 版本管理

我们使用 [SemVer](http://semver.org/) 规范做版本管理. 历史发布版本通过tag管理, 参考 [本项目tag列表](https://git.code.oa.com/tdbank/tdbank-agent/-/tags)


## License

本项目版本许可说明 [LICENSE.md](LICENSE.md)
