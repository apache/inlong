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

BASE_DIR=$(dirname "$0")
PY_SDK_DIR=$(cd "$BASE_DIR"; pwd)

echo "InLong Python SDK Uninstaller"
echo "=============================="
echo "This script will uninstall the InLong Python SDK by removing the following files:"
echo "  - inlong_dataproxy*.so"
echo "  - inlong_dataproxy*.so.*"
echo "from your Python site-packages directories."

# 获取所有 Python site-packages 目录
SITE_PACKAGES_DIRS=($(python -c "import site,os; print(' '.join([p for p in site.getsitepackages() if os.path.isdir(p)]))"))

if [ ${#SITE_PACKAGES_DIRS[@]} -ne 0 ]; then
    echo "Your system's existing Python site-packages directories are:"
    for dir in "${SITE_PACKAGES_DIRS[@]}"; do
        echo "  $dir"
    done
else
    echo "Warn: No existing site-packages directories found, please enter the target directory for the .so files in the following step!"
fi

read -r -p "Enter the target directory to uninstall from (Press Enter to search in all above site-packages directories): " target_dir

SO_FILE_PATTERNS=("inlong_dataproxy*.so" "inlong_dataproxy*.so.*")

uninstall_from_dir() {
    local dir=$1
    echo "Searching for InLong Python SDK files in $dir..."
    local so_files=()
    for pattern in "${SO_FILE_PATTERNS[@]}"; do
        so_files+=($(find "$dir" -name "$pattern" 2>/dev/null))
    done
    if [ ${#so_files[@]} -eq 0 ]; then
        echo "  No InLong Python SDK files found in $dir"
        return 0
    fi
    echo "  Found ${#so_files[@]} InLong Python SDK files:"
    for file in "${so_files[@]}"; do
        echo "    $file"
    done
    read -r -p "Do you want to delete these files? (y/n): " confirm
    if [[ $confirm =~ ^[Yy]$ ]]; then
        for file in "${so_files[@]}"; do
            rm -f "$file"
            echo "  Deleted: $file"
        done
        echo "  Uninstallation from $dir completed successfully."
    else
        echo "  Uninstallation from $dir cancelled."
    fi
}

if [ -z "$target_dir" ]; then
    for dir in "${SITE_PACKAGES_DIRS[@]}"; do
        uninstall_from_dir "$dir"
    done
else
    if [ ! -d "$target_dir" ]; then
        echo "Error: Directory $target_dir does not exist!"
        exit 1
    fi
    uninstall_from_dir "$target_dir"
fi

echo "InLong Python SDK uninstallation process completed."