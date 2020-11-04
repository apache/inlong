### TubeMQ Python Client
TubeMQ Python Client library is a wrapper over the existing [C++ client library](https://github.com/apache/incubator-tubemq/tree/master/tubemq-client-twins/tubemq-client-cpp/) and exposes all of the same features.

#### Install from source
##### install python-devel
- build and install C++ client SDK

build C++ client SDK from source, and install:

1, copy `tubemq` include directory  to `/usr/local/include/`

2, copy `libtubemq_rel.a` to `/usr/local/lib`
&nbsp;

- install python-devel
```
yum install python-devel -y
```
- install required dependency
```
pip install -r requirements.txt
```

- install client
```
pip install ./
```

#### Examples
##### Producer example
##### Consumer example
Consumer example is [available](https://github.com/apache/incubator-tubemq/tree/tubemq-client-python/tubemq-client-twins/tubemq-client-python/src/python/example/consumer).