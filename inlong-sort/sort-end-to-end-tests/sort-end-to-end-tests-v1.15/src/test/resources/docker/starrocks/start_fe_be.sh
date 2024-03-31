# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#!/bin/bash

log_stdin()
{
    echo "[`date`] $@" >&1
}
log_stdin `cat /etc/hosts`

cp /etc/hosts /etc/hosts.temp
sed -i 'N;$!P;D' /etc/hosts.temp
cat /etc/hosts.temp > /etc/hosts

log_stdin `cat /etc/hosts`
sleep 2
# Start FE.
cd $SR_HOME/fe/bin/

log_stdin "Start FE"
./start_fe.sh --host_type FQDN --daemon

# Start BE.
log_stdin "Start BE"
cd $SR_HOME/be/bin/
./start_be.sh --daemon

# Sleep until the cluster starts.
sleep 20;

# Fetch fqdn with the command suggested by AWS official doc: https://docs.aws.amazon.com/managedservices/latest/userguide/find-FQDN.html
MYFQDN=`hostname --fqdn`
log_stdin "Register BE ${MYFQDN} to FE"
mysql -uroot -h${MYFQDN} -P 9030 -e "alter system add backend '${MYFQDN}:9050';"

# Sleep until the BE register to FE.
sleep 5;

log_stdin "init starrocks db begin..."
mysql -uroot -h${MYFQDN} -P 9030  <<EOF
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE ROLE inlong;
GRANT CREATE TABLE, ALTER, DROP ON DATABASE test TO ROLE inlong WITH GRANT OPTION;
GRANT SELECT, ALTER, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE test TO ROLE 'inlong' WITH GRANT OPTION;
CREATE USER 'inlong'@'%' IDENTIFIED BY 'inlong' DEFAULT ROLE 'inlong';
CREATE USER 'flinkuser'@'%' IDENTIFIED BY 'flinkpw' DEFAULT ROLE 'inlong';
EOF
log_stdin "init starrocks db end!"

# health check the entire stack end-to-end and exit on failure.
while sleep 10; do
  PROCESS_STATUS=`mysql -uroot -h${MYFQDN} -P 9030 -e "show backends\G" |grep "Alive: true"`
  if [ -z "$PROCESS_STATUS" ]; then
        log_stdin "service has exited"
        exit 1;
  fi;
  log_stdin $PROCESS_STATUS
done
