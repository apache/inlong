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

# Detect Python command
PYTHON_CMD=""
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    echo "Error: Python is not installed or not found in PATH"
    exit 1
fi

# Get all Python site-packages directories
SITE_PACKAGES_DIRS=()
SITE_PACKAGES_OUTPUT=$($PYTHON_CMD -c "import site,os; print(' '.join([p for p in site.getsitepackages() if os.path.isdir(p)]))" 2>/dev/null)
if [ $? -eq 0 ] && [ -n "$SITE_PACKAGES_OUTPUT" ]; then
    SITE_PACKAGES_DIRS=($SITE_PACKAGES_OUTPUT)
fi

# Also check user site-packages
USER_SITE_PACKAGES=$($PYTHON_CMD -c "import site; print(site.getusersitepackages())" 2>/dev/null || echo "")
if [ -n "$USER_SITE_PACKAGES" ] && [ -d "$USER_SITE_PACKAGES" ]; then
    SITE_PACKAGES_DIRS+=("$USER_SITE_PACKAGES")
fi

if [ ${#SITE_PACKAGES_DIRS[@]} -ne 0 ]; then
    echo "Your system's existing Python site-packages directories are:"
    for dir in "${SITE_PACKAGES_DIRS[@]}"; do
        echo "  $dir"
    done
else
    echo "Warning: No existing site-packages directories found."
    echo "Please enter the target directory for the .so files in the following step!"
fi

read -r -p "Enter the target directory to uninstall from (Press Enter to search in all above site-packages directories): " target_dir

SO_FILE_PATTERNS=("inlong_dataproxy*.so" "inlong_dataproxy*.so.*")

uninstall_from_dir() {
    local dir=$1
    echo "Searching for InLong Python SDK files in $dir..."
    
    # Check if directory exists and is writable
    if [ ! -d "$dir" ]; then
        echo "  Warning: Directory $dir does not exist"
        return 1
    fi
    
    if [ ! -w "$dir" ]; then
        echo "  Warning: Directory $dir is not writable. You may need to run with sudo."
        return 1
    fi
    
    local so_files=()
    for pattern in "${SO_FILE_PATTERNS[@]}"; do
        while IFS= read -r -d '' file; do
            so_files+=("$file")
        done < <(find "$dir" -name "$pattern" -print0 2>/dev/null)
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
        local deleted_count=0
        for file in "${so_files[@]}"; do
            if rm -f "$file" 2>/dev/null; then
                echo "  Deleted: $file"
                ((deleted_count++))
            else
                echo "  Failed to delete: $file (permission denied or file not found)"
            fi
        done
        echo "  Uninstallation from $dir completed. Deleted $deleted_count files."
    else
        echo "  Uninstallation from $dir cancelled."
    fi
}

if [ -z "$target_dir" ]; then
    if [ ${#SITE_PACKAGES_DIRS[@]} -eq 0 ]; then
        echo "Error: No site-packages directories found and no target directory specified."
        echo "Please specify a target directory manually."
        exit 1
    fi
    
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