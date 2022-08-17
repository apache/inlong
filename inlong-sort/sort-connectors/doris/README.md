# Overview
The sort-connector-doris connector comes from doris-flink-connector officially provided by apache doris.
The flink-doris-connector-1.13_2.12 version is currently introduced. If you need to use scala 2.11, you need to 
download the flink-doris-connector source code of the apache doris community and checkout a new branch from 
tag 1.13_2.11-1.0.3.

# Checkout a new branch
checkout new branch: `git checkout -b 1.13_2.11-1.0.3`

# Install thrift 0.13.0
The flink-doris-connector source code uses thrift, and thrift needs to be installed before compiling.
The installation steps are as follows:

## Windows:
1. Download: `http://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.exe`
2. Modify thrift-0.13.0.exe to thrift

## MacOS:
1. Download: `brew install thrift@0.13.0`
2. Default address: /opt/homebrew/Cellar/thrift@0.13.0/0.13.0/bin/thrift

Note: Executing `brew install thrift@0.13.0` on MacOS may report an error that the version cannot be found. 
The solution is as follows, execute it in the terminal:
1. `brew tap-new $USER/local-tap`
2. `brew extract --version='0.13.0' thrift $USER/local-tap`
3. `brew install thrift@0.13.0`
   Reference link: `https://gist.github.com/tonydeng/02e571f273d6cce4230dc8d5f394493c`

## Linux:
1. Download source package：`wget https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz`
2. Install dependencies：`yum install -y autoconf automake libtool cmake ncurses-devel openssl-devel 
lzo-devel zlib-devel gcc gcc-c++`
3. `tar zxvf thrift-0.13.0.tar.gz`
4. `cd thrift-0.13.0`
5. `./configure --without-tests`
6. `make`
7. `make install`

Check the version after installation is complete：thrift --version

# Package flink-doris-connector
```
mvn clean package -Dscala.version=2.11 -Dflink.version=1.13.5 -Dflink.minor.version=1.13 
-Denv.THRIFT_BIN=/path/to/thrift
```

# Remark
For more information on using sort-connector-doris, 
see [flink-doris-connector](https://doris.apache.org/docs/1.0/extending-doris/flink-doris-connector/)