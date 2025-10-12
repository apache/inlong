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
# Initialize the configuration files of inlong components
#

set -e

# Parse command line arguments
TARGET_DIR=""
if [ $# -eq 1 ]; then
    TARGET_DIR="$1"
    echo "Using user-specified target directory: $TARGET_DIR"
    # Check if the target directory exists early to avoid wasting compilation time
    if [ ! -d "$TARGET_DIR" ]; then
        echo "Error: Target directory '$TARGET_DIR' does not exist!"
        exit 1
    fi
elif [ $# -gt 1 ]; then
    echo "Usage: $0 [target_directory]"
    echo "  target_directory: Optional. Directory to install .so files. If not provided, will use system site-packages directories."
    exit 1
fi

BASE_DIR=$(dirname "$0")
PY_SDK_DIR=$(cd "$BASE_DIR"; pwd)

echo "The python sdk directory is: $PY_SDK_DIR"

# Check if dataproxy-sdk-cpp directory exists in the parent directory
if [ ! -d "$PY_SDK_DIR/../dataproxy-sdk-cpp" ]; then
    echo "Error: cannot find the dataproxy-cpp-sdk directory! The dataproxy-cpp-sdk directory must be located in the same directory as the dataproxy-python-sdk directory."
    exit 1
fi

CPP_SDK_DIR=$(cd "$PY_SDK_DIR/../dataproxy-sdk-cpp"; pwd)

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

# Install Python packages from requirements.txt
if [ -f "$PY_SDK_DIR/requirements.txt" ]; then
    echo "Installing Python packages from requirements.txt..."
    pip install -r "$PY_SDK_DIR/requirements.txt"
else
    echo "Error: cannot find requirements.txt!"
    exit 1
fi

# Build pybind11(If the pybind11 has been compiled, this step will be skipped)
if [ ! -d "$PY_SDK_DIR/pybind11/build" ]; then
    if [ -d "$PY_SDK_DIR/pybind11" ]; then
        rm -r "$PY_SDK_DIR/pybind11"
    fi
    PYBIND11_VERSION="v2.13.0"
    git clone --branch $PYBIND11_VERSION --depth 1 https://github.com/pybind/pybind11.git "$PY_SDK_DIR/pybind11"
    mkdir "$PY_SDK_DIR/pybind11/build" && cd "$PY_SDK_DIR/pybind11/build"

    # Add a trap command to delete the pybind11 folder if an error occurs
    trap 'echo "Error occurred during pybind11 build. Deleting pybind11 folder..."; cd $PY_SDK_DIR; rm -r pybind11; exit 1' ERR

    cmake "$PY_SDK_DIR/pybind11"
    cmake --build "$PY_SDK_DIR/pybind11/build" --config Release
    make -j 4

    # Remove the trap command if the build is successful
    trap - ERR
else
    echo "Skipped build pybind11"
fi

# Build dataproxy-sdk-cpp(If the dataproxy-sdk-cpp has been compiled, this step will be skipped)
if [ ! -e "$CPP_SDK_DIR/release/lib/dataproxy_sdk.a" ]; then
    echo "The dataproxy-sdk-cpp is not compiled, you should run the following commands to compile it first:"
    echo "----------------------------------------------------------------------------------------------"
    echo "cd $CPP_SDK_DIR && chmod +x build_third_party.sh && chmod +x build.sh"
    echo "./build_third_party.sh"
    echo "./build.sh"
    echo "----------------------------------------------------------------------------------------------"
    exit 1
else
    if [ -d "$PY_SDK_DIR/dataproxy-sdk-cpp" ]; then
        rm -r "$PY_SDK_DIR/dataproxy-sdk-cpp"
    fi
    cp -r "$CPP_SDK_DIR" "$PY_SDK_DIR"
    echo "Copied the dataproxy-sdk-cpp directory to the current directory"
fi

# Build Python SDK
if [ -d "$PY_SDK_DIR/build" ]; then
    rm -r "$PY_SDK_DIR/build"
fi
mkdir "$PY_SDK_DIR/build" && cd "$PY_SDK_DIR/build"
cmake "$PY_SDK_DIR"
make -j 4

# Handle installation based on command line arguments
if [ -n "$TARGET_DIR" ]; then
    # User specified a target directory via command line argument
    echo "Copying .so files to user-specified directory: $TARGET_DIR"
    find "$PY_SDK_DIR/build" -name "*.so" -print0 | xargs -0 -I {} cp {} "$TARGET_DIR"
else
    # No command line argument provided, use system site-packages directories
    # Get all existing Python site-packages directories
    SITE_PACKAGES_DIRS=($(python -c "import site,os; print(' '.join([p for p in site.getsitepackages() if os.path.isdir(p)]))"))
    if [ ${#SITE_PACKAGES_DIRS[@]} -ne 0 ]; then
        echo "No target directory specified, using system site-packages directories:"
        for dir in "${SITE_PACKAGES_DIRS[@]}"; do
            echo "  $dir"
        done
        for dir in "${SITE_PACKAGES_DIRS[@]}"; do
            echo "Copying .so files to $dir"
            # Find all .so files in $PY_SDK_DIR/build and copy them to the current site-packages directory
            find "$PY_SDK_DIR/build" -name "*.so" -print0 | xargs -0 -I {} cp {} "$dir"
        done
    else
        echo "Error: No system site-packages directories found and no target directory specified!"
        echo "The .so file is located in $PY_SDK_DIR/build, you can copy it manually to your project"
    fi
fi

# Clean the cpp dataproxy directory
rm -r "$PY_SDK_DIR/dataproxy-sdk-cpp"

echo "Build Python SDK successfully"