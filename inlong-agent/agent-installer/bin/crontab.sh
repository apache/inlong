#!/bin/sh

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
