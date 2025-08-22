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

set -e

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
    cmake --build "$PY_SDK_DIR/pybind11/build" --config Release --target check
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

# Get all existing Python site-packages directories
SITE_PACKAGES_DIRS=($(python -c "import site,os; print(' '.join([p for p in site.getsitepackages() if os.path.isdir(p)]))"))

# Check if the SITE_PACKAGES_DIRS array is not empty
if [ ${#SITE_PACKAGES_DIRS[@]} -ne 0 ]; then
    # If not empty, display all found site-packages directories to the user
    echo "Your system's existing Python site-packages directories are:"
    for dir in "${SITE_PACKAGES_DIRS[@]}"; do
        echo "  $dir"
    done
else
    # If empty, warn the user and prompt them to enter the target directory in the next step
    echo "Warn: No existing site-packages directories found, please enter the target directory for the .so files in the following step!"
fi

# Prompt user for the target directory for .so files
read -r -p "Enter the target directory for the .so files (Press Enter to use all above site-packages directories): " target_dir

# If user input is empty, use all found site-packages directories
if [ -z "$target_dir" ]; then
    for dir in "${SITE_PACKAGES_DIRS[@]}"; do
        echo "Copying .so files to $dir"
        # Find all .so files in $PY_SDK_DIR/build and copy them to the current site-packages directory
        find "$PY_SDK_DIR/build" -name "*.so" -print0 | xargs -0 -I {} cp {} "$dir"
    done
else
    # If user specified a directory, copy .so files there
    echo "Copying .so files to $target_dir"
    find "$PY_SDK_DIR/build" -name "*.so" -print0 | xargs -0 -I {} cp {} "$target_dir"
fi

# Clean the cpp dataproxy directory
rm -r "$PY_SDK_DIR/dataproxy-sdk-cpp"

echo "Build Python SDK successfully"