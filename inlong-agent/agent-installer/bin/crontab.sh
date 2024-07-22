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

BASE_DIR=$(cd "$(dirname "$0")"/../;pwd)
cd $BASE_DIR

LOG_DIR=${BASE_DIR}/logs/

CRON_CMD="*/5 * * * * (cd ${BASE_DIR};sh ./bin/installer.sh start > ${BASE_DIR}/logs/monitor.log 2>&1)"

CRON_COUNT=`crontab -l | grep -Ew "${BASE_DIR}" | grep "^[^#+]" | wc -l`
if [ $CRON_COUNT -eq 0 ]; then
	mkdir -p $LOG_DIR
	CRON_TMP=${LOG_DIR}/crontab.tmp
	crontab -l > $CRON_TMP
	echo "${CRON_CMD}" >> $CRON_TMP
	crontab $CRON_TMP
fi
