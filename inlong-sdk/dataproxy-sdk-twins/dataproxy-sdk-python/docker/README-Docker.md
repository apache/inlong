# DataProxy SDK Python Docker Build Environment

Docker image providing GCC, CMake, Git, and Python environment for building dataproxy-sdk-python.

## Environment

- **OS**: CentOS 7
- **GCC**: System default (4.8.5)
- **CMake**: 3.12.4
- **Git**: 2.34.1
- **Curl**: 7.78.0
- **Python**: Configurable version (default: 3.8.0)
- **Tools**: gcc, gcc-c++, make, autoconf, automake, libtool, pkgconfig, openssl-devel, zlib-devel, etc.

## Build Docker Image

### Default Python Version (3.8.0)

```bash
docker build -t inlong/dataproxy-python-compile .
```

### Custom Python Version

You can specify a different Python version(>=3.7) using build arguments:

```bash
# For Python 3.9.18
docker build --build-arg PYTHON_VERSION=3.9.18 -t inlong/dataproxy-python-compile:py39 .

# For Python 3.10.13
docker build --build-arg PYTHON_VERSION=3.10.13 -t inlong/dataproxy-python-compile:py310 .

# For Python 3.11.7
docker build --build-arg PYTHON_VERSION=3.11.7 -t inlong/dataproxy-python-compile:py311 .
```

## Usage

### Basic Usage

**Important**: You must mount the entire `dataproxy-sdk-twins` directory (not just the Python SDK directory) because the Python SDK depends on the C++ SDK.

```bash
docker run -v /path/to/your/dataproxy-sdk-twins:/dataproxy-sdk-twins inlong/dataproxy-python-compile
```

### Example

```bash
# Using current directory (assuming you're in dataproxy-sdk-twins directory)
docker run -v $(pwd):/dataproxy-sdk-twins inlong/dataproxy-python-compile

# Using absolute path
docker run -v /home/user/projects/inlong/inlong-sdk/dataproxy-sdk-twins:/dataproxy-sdk-twins inlong/dataproxy-python-compile
```

### Build with Custom Python Version

```bash
# Build and use Python 3.9 version
docker build --build-arg PYTHON_VERSION=3.9.18 -t inlong/dataproxy-python-compile:py39 .
docker run -v /home/user/projects/inlong/inlong-sdk/dataproxy-sdk-twins:/dataproxy-sdk-twins inlong/dataproxy-python-compile:py39
```

## Build Output

Build artifacts will be available in the `dataproxy-sdk-python/build/` subdirectory of your source code:

- `inlong_dataproxy.cpython-*.so` - The compiled Python Dataproxy SDK module