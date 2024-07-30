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

BASE_DIR=`dirname "$0"`
PY_SDK_DIR=`cd "$BASE_DIR";pwd`

echo "The python sdk directory is: $PY_SDK_DIR"

# Check if dataproxy-sdk-cpp directory exists in the parent directory
if [ ! -d "$PY_SDK_DIR/../dataproxy-sdk-cpp" ]; then
    echo "Error: cannot find the dataproxy-cpp-sdk directory! The dataproxy-cpp-sdk directory must be located in the same directory as the dataproxy-python-sdk directory."
    exit 1
fi

CPP_SDK_DIR=`cd "$PY_SDK_DIR/../dataproxy-sdk-cpp";pwd`

echo "The cpp sdk directory is: $CPP_SDK_DIR"

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
git clone https://github.com/pybind/pybind11.git $PY_SDK_DIR/pybind11
mkdir $PY_SDK_DIR/pybind11/build && cd $PY_SDK_DIR/pybind11/build
cmake $PY_SDK_DIR/pybind11
cmake --build $PY_SDK_DIR/pybind11/build --config Release --target check
make check -j 4

# Build dataproxy-sdk-cpp(If the dataproxy-sdk-cpp has been compiled, this step will be skipped)
if [ ! -e "$CPP_SDK_DIR/release/lib/dataproxy_sdk.a" ]; then
    chmod +x $CPP_SDK_DIR/build.sh
    cd $CPP_SDK_DIR
    . $CPP_SDK_DIR/build.sh
    cp -r $CPP_SDK_DIR $PY_SDK_DIR
else
    cp -r $CPP_SDK_DIR $PY_SDK_DIR
    echo "Skipped build dataproxy-sdk-cpp"
fi

# Build Python SDK
if [ -d "$PY_SDK_DIR/build" ]; then
    rm -r $PY_SDK_DIR/build
fi
mkdir $PY_SDK_DIR/build && cd $PY_SDK_DIR/build
cmake $PY_SDK_DIR
make

# Get Python site-packages directory
SITE_PACKAGES_DIR=$(python -c "import site; print(site.getsitepackages()[0])")

# Copy generated .so file to site-packages directory
find $PY_SDK_DIR/build -name "*.so" -print0 | xargs -0 -I {} bash -c 'rm -f $0/$1; cp $1 $0' $SITE_PACKAGES_DIR {}

# Clean
rm -r $PY_SDK_DIR/pybind11 $PY_SDK_DIR/dataproxy-sdk-cpp