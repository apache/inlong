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

script_dir=$(dirname "$0")
script_file_name=$(basename "$0")

# the absolute dir of project
base_dir=$(
  cd $script_dir
  cd ../..
  pwd
)

help_action=(
  'help'
  'h'
  'help: show all actions'
  'welcome'
)

manager_plugins_action=(
  'manager_plugins'
  'mp'
  'build manager local debugging environment'
  'manager_plugins'
)

actions=(
  help_action
  manager_plugins_action
)

function welcome() {
  local_date_time=$(date +"%Y-%m-%d %H:%M:%S")
  echo '####################################################################################'
  echo '#                 Welcome to use Apache InLong dev toolkit !                       #'
  echo '#                                        @'$local_date_time'                      #'
  echo '####################################################################################'
  echo ''

  # shellcheck disable=SC2068
  for action in ${actions[@]}; do
    # shellcheck disable=SC1087
    TMP=$action[@]
    TempB=("${!TMP}")
    name=${TempB[0]}
    simple_name=${TempB[1]}
    desc=${TempB[2]}

    echo $script_dir'/'$script_file_name' '$name' | '$simple_name
    echo '      :'$desc
  done
  echo ''
}

function manager_plugins() {
  echo '# start build manager local debugging environment ...'

  project_version=$(mvn -q \
    -Dexec.executable=echo \
    -Dexec.args='${project.version}' \
    --non-recursive \
    exec:exec)

  echo 'current_version: '"$project_version"
  #
  echo 'associate plugins directory: inlong-manager/manager-plugins/target/plugins'
  # plugins -> manager-plugins/target/plugins
  cd "$base_dir"

 # create dev directory if absent
  if [ ! -d dev  ];then
    mkdir dev
  else
    echo The directory is exist: dev
  fi
  # associate plugins
  rm -rf dev/plugins
  ln -s inlong-manager/manager-plugins/target/plugins dev/plugins

  echo 'build dev env of manager finished .'
}

function main() {
  action=$1

  if [ ! -n "$action" ]; then
    welcome
  fi

  # shellcheck disable=SC2068
  for one_action in ${actions[@]}; do
    # shellcheck disable=SC1087
    TMP=$one_action[@]
    TempB=("${!TMP}")
    name=${TempB[0]}
    simple_name=${TempB[1]}
    function_name=${TempB[3]}
    desc=${TempB[4]}

    if [[ X"$name" == X"$action" ]] || [[ X"$simple_name" == X"$action" ]]; then
      echo 'Execute action: '"$function_name"
      $function_name
    fi
  done

  echo 'Have a nice day, bye!'
}

main $1
