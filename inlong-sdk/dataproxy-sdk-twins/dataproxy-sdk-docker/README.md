## Docker Image for Building DataProxy SDK

This image provides a complete build environment for compiling InLong DataProxy C++ and Python SDKs.

### Requirements

- [Docker](https://docs.docker.com/engine/install/) 19.03.1+

### Pull Image

```bash
docker pull inlong/dataproxy-sdk-build:latest
```

### Build SDKs

This image supports building both InLong Dataproxy C++ and Python SDKs. Python SDK is a wrapper over the C++ SDK and requires C++ SDK to be built first.

#### Option 1: Interactive Build

```bash
# Run container
docker run -it inlong/dataproxy-sdk-build:latest

# Inside container: clone and build C++ SDK
git clone https://github.com/apache/inlong.git
cd inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-cpp
./build_third_party.sh && ./build.sh

# [Optional] Build Python SDK (requires C++ SDK built first)
cd ../dataproxy-sdk-python
./build.sh
# Verify: python -c "import inlong_dataproxy"
```

#### Option 2: Mount Source Code

```bash
# Mount your local InLong source code
docker run -it -v /path/to/inlong:/workspace/inlong inlong/dataproxy-sdk-build:latest bash

# Inside container: build C++ SDK
cd /workspace/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-cpp
./build_third_party.sh && ./build.sh

# [Optional] Build Python SDK
cd ../dataproxy-sdk-python
./build.sh
```

### Build Your Own Image

#### Basic Build

```bash
cd inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-docker
docker build -t inlong/dataproxy-sdk-build:latest .
```

#### Custom Python Version

```bash
docker build --build-arg PYTHON_VERSION=3.9.18 -t inlong/dataproxy-sdk-build:py39 .
```
