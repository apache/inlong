#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
cd "$(dirname "$0")"/../ || exit

filelist() {
    rm -fr $1/dependencys.txt
    for file in $1/*
    do
        if [[ -f $file ]]; then
            if [[ $file == *".jar" ]]; then
                filename=$(basename "$file")
                echo $filename >> $1/dependencys.txt
            fi
        fi
    done
}

# Get the current version for InLong
prefix="apache-inlong-"
suffix="-bin.tar.gz"
find_gz_file=$(ls -l ./target/*bin.tar.gz |awk '{print $9}')
gz_file=$(basename "$find_gz_file")
version_with_suffix="${gz_file#$prefix}"
project_version="${version_with_suffix%$suffix}"
echo "The current version for InLong is: $project_version"
projectpath="./target/apache-inlong-${project_version}-bin/apache-inlong-${project_version}"

# Generate the file "dependencys.txt"
filelist "./$projectpath/inlong-agent/lib"
filelist "./$projectpath/inlong-dataproxy/lib"
filelist "./$projectpath/inlong-manager/lib"
filelist "./$projectpath/inlong-tubemq-server/lib"
filelist "./$projectpath/inlong-tubemq-manager/lib"
filelist "./$projectpath/inlong-audit/lib"

# Move every module jar file to same lib directory
mkdir -p $projectpath/lib
mv $projectpath/inlong-agent/lib/*.jar $projectpath/lib/
mv $projectpath/inlong-dataproxy/lib/*.jar $projectpath/lib/
mv $projectpath/inlong-manager/lib/*.jar $projectpath/lib/
mv $projectpath/inlong-tubemq-server/lib/*.jar $projectpath/lib/
mv $projectpath/inlong-tubemq-manager/lib/*.jar $projectpath/lib/
mv $projectpath/inlong-audit/lib/*.jar $projectpath/lib/

# Copy the file "copy_module_dependencies.sh" to bin directory
cp ./script/copy_module_dependencies.sh $projectpath/bin/
chmod 755 $projectpath/bin/copy_module_dependencies.sh

# Copy the file "prepare_module_dependencies.sh" to bin directory
cp ./script/prepare_module_dependencies.sh $projectpath/bin/
chmod 755 $projectpath/bin/prepare_module_dependencies.sh

# Package InLong version file
lastname=$(basename "$projectpath")
cd $projectpath/..
tar cvf $lastname-bin.tar $lastname
gzip $lastname-bin.tar
mv $lastname-bin.tar.gz ../
