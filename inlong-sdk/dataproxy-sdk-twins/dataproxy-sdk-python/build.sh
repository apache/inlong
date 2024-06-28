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
# Initialize the configuration files of inlong components
#

#!/bin/bash

# Check CMake version
CMAKE_VERSION=$(cmake --version | head -n 1 | cut -d " " -f 3)
CMAKE_REQUIRED="3.5"
if [ "$(printf '%s\n' "$CMAKE_REQUIRED" "$CMAKE_VERSION" | sort -V | head -n1)" != "$CMAKE_REQUIRED" ]; then
    echo "CMake version must be greater than or equal to $CMAKE_REQUIRED"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$(python --version 2>&1 | cut -d " " -f 2)
PYTHON_REQUIRED="3.6"
if [ "$(printf '%s\n' "$PYTHON_REQUIRED" "$PYTHON_VERSION" | sort -V | head -n1)" != "$PYTHON_REQUIRED" ]; then
    echo "Python version must be greater than or equal to $PYTHON_REQUIRED"
    exit 1
fi

# Clone and build pybind11
git clone https://github.com/pybind/pybind11.git
cd pybind11
mkdir build
cd build
cmake ..
cmake --build . --config Release --target check
make check -j 4
cd ../..

# Clone and build dataproxy-sdk-cpp
git clone https://github.com/apache/inlong.git
mv ./inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-cpp ./
rm -r ./inlong
cd ./dataproxy-sdk-cpp
chmod +x ./build.sh
./build.sh
cd ..

# Build Python SDK
if [ -d "./build" ]; then
    rm -r ./build
fi
mkdir build
cd build
cmake ..
make
cd ..

# Get Python site-packages directory
SITE_PACKAGES_DIR=$(python -c "import site; print(site.getsitepackages()[0])")

# Copy generated .so file to site-packages directory
find ./build -name "*.so" -print0 | xargs -0 -I {} bash -c 'rm -f $0/$1; cp $1 $0' $SITE_PACKAGES_DIR {}

# Clean
rm -r ./pybind11
rm -r ./dataproxy-sdk-cpp