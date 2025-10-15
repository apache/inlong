#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

PY_SDK_DIR="/dataproxy-sdk-twins/dataproxy-sdk-python"

# Check if dataproxy-sdk-cpp directory exists in the mounted directory
if [ ! -d "/dataproxy-sdk-twins/dataproxy-sdk-cpp" ]; then
    echo "Error: cannot find the dataproxy-sdk-cpp directory!"
    echo "Expected path: /dataproxy-sdk-twins/dataproxy-sdk-cpp"
    echo "Please ensure you have mounted the dataproxy-sdk-twins directory correctly:"
    echo "  docker run -v /path/to/dataproxy-sdk-twins:/dataproxy-sdk-twins inlong/dataproxy-python-compile"
    exit 1
fi

CPP_SDK_DIR="/dataproxy-sdk-twins/dataproxy-sdk-cpp"

echo "Building Python SDK"

# Install Python packages from requirements.txt
if [ -f "$PY_SDK_DIR/requirements.txt" ]; then
    echo "Installing Python packages from requirements.txt..."
    pip install -r "$PY_SDK_DIR/requirements.txt"
else
    echo "Warning: cannot find requirements.txt, skipping Python package installation"
fi

# Build pybind11 from source (If the pybind11 has been compiled, this step will be skipped)
if [ ! -d "$PY_SDK_DIR/pybind11/build" ]; then
    if [ -d "$PY_SDK_DIR/pybind11" ]; then
        rm -r "$PY_SDK_DIR/pybind11"
    fi
    # Use pybind11 v2.10.4 for better compatibility with GCC 4.8.5
    PYBIND11_VERSION="v2.10.4"
    git clone --branch $PYBIND11_VERSION --depth 1 https://github.com/pybind/pybind11.git "$PY_SDK_DIR/pybind11"
    mkdir "$PY_SDK_DIR/pybind11/build" && cd "$PY_SDK_DIR/pybind11/build"

    # Add a trap command to delete the pybind11 folder if an error occurs
    trap 'echo "Error occurred during pybind11 build. Deleting pybind11 folder..."; cd $PY_SDK_DIR; rm -r pybind11; exit 1' ERR

    cmake "$PY_SDK_DIR/pybind11" \
        -DCMAKE_BUILD_TYPE=Release \
        -DPYBIND11_TEST=OFF

    # Build pybind11 library
    make -j"$(nproc)"

    # Remove the trap command if the build is successful
    trap - ERR
else
    echo "Skipped build pybind11"
fi

# Build dataproxy-sdk-cpp (If the dataproxy-sdk-cpp has been compiled, this step will be skipped)
if [ ! -e "$CPP_SDK_DIR/release/lib/dataproxy_sdk.a" ]; then
    echo "The dataproxy-sdk-cpp is not compiled, building it now..."
    echo "----------------------------------------------------------------------------------------------"
    cd "$CPP_SDK_DIR"
    chmod +x build_third_party.sh && chmod +x build.sh
    ./build_third_party.sh
    ./build.sh
    cd "$PY_SDK_DIR"
    echo "----------------------------------------------------------------------------------------------"
else
    echo "dataproxy-sdk-cpp already compiled, skipping C++ build"
fi

# Copy dataproxy-sdk-cpp to current directory
if [ -d "$PY_SDK_DIR/dataproxy-sdk-cpp" ]; then
    rm -r "$PY_SDK_DIR/dataproxy-sdk-cpp"
fi
cp -r "$CPP_SDK_DIR" "$PY_SDK_DIR"
echo "Copied the dataproxy-sdk-cpp directory to the current directory"

# Build Python SDK
if [ -d "$PY_SDK_DIR/build" ]; then
    rm -r "$PY_SDK_DIR/build"
fi
mkdir "$PY_SDK_DIR/build" && cd "$PY_SDK_DIR/build"

cmake "$PY_SDK_DIR" -DCMAKE_BUILD_TYPE=Release

make -j"$(nproc)"

echo "=== Build Summary ==="
echo "Build completed successfully!"
echo "Generated .so files are located in: $PY_SDK_DIR/build/"
echo "Available .so files:"
find "$PY_SDK_DIR/build" -name "*.so" -type f

rm -r "$PY_SDK_DIR/dataproxy-sdk-cpp"

echo "Build Python SDK successfully"