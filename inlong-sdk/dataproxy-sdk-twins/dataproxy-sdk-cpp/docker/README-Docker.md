# DataProxy SDK C++ Docker Compile Environment

Docker image providing GCC, CMake, and Git environment for compiling C++ dataproxy SDK.

## Environment

- **OS**: CentOS 7
- **GCC**: System default (4.8.5)
- **CMake**: 3.12.4
- **Git**: 2.34.1
- **Curl**: 7.78.0
- **Tools**: gcc, gcc-c++, make, autoconf, automake, libtool, pkgconfig, openssl-devel, zlib-devel

## Build Docker Image

Navigate to the `dataproxy-sdk-cpp/docker` directory and build the image:

```bash
cd docker
docker build -t inlong/dataproxy-cpp-compile .
```

## Usage

### Basic Usage

Run from the `dataproxy-sdk-cpp` directory:

```bash
docker run -v $(pwd):/dataproxy-sdk-cpp inlong/dataproxy-cpp-compile
```

### Example

```bash
# Navigate to the dataproxy-sdk-cpp directory
cd /path/to/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-cpp

# Run the build
docker run -v $(pwd):/dataproxy-sdk-cpp inlong/dataproxy-cpp-compile
```

### Alternative Usage

You can also run from any directory by specifying the full path:

```bash
docker run -v /path/to/dataproxy-sdk-cpp:/dataproxy-sdk-cpp inlong/dataproxy-cpp-compile
```

## Output

Build artifacts will be available in the following directories of your source code:
- `build/` - CMake build directory with object files and intermediate artifacts
- `release/` - Final release artifacts and libraries