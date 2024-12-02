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

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime
import os
import logging
import pytz
from croniter import croniter
from airflow.hooks.base_hook import BaseHook
from airflow import configuration

DAG_PATH = configuration.get('core', 'dags_folder') + "/"
DAG_PREFIX = 'inlong_offline_task_'

def clean_expired_dags(**context):

    original_time = context.get('execution_date')
    target_timezone = pytz.timezone("Asia/Shanghai")
    utc_time = original_time.astimezone(target_timezone)
    current_time = utc_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    logging.info(f"The execution time of this cleaning task is: {current_time}")

    conf = context.get('dag_run').conf
    logging.info(f"Execution parameters for this cleaning task: {conf}")
    groupId = conf.get('inlong_group_id')

    if groupId is None or len(groupId) == 0:
        for dag_file in os.listdir(DAG_PATH):
            if dag_file.endswith(".py") and dag_file.startswith(DAG_PREFIX):
                dag_file_path = os.path.join(DAG_PATH, dag_file)
                with open(dag_file_path, "r") as file:
                    line = file.readline()
                    while line and "end_offset_datetime_str" not in line:
                        line = file.readline()
                    end_date_str = None
                    row = line.split("=")
                    if len(row) > 1:
                        end_date_str = datetime.fromtimestamp(int(row[1].strip().strip("\"")) / 1000, tz=target_timezone)
                    logging.info(f"The end time of '{dag_file}' is: {end_date_str}")
                    try:
                        if end_date_str and str(current_time) > str(end_date_str):
                            os.remove(dag_file_path)
                            logging.info(f"Deleted expired DAG: {dag_file}")
                    except ValueError:
                        logging.error(f"Failed to delete {dag_file}: {end_date_str}")
    else:
        dag_file = groupId + '.py'
        if not str(groupId).startswith(DAG_PREFIX):
            dag_file = DAG_PREFIX + dag_file
        os.remove(os.path.join(DAG_PATH, dag_file))
        logging.info(f"Deleted expired DAG: {dag_file}")



default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=5),
    'catchup': False,
    'tags': ["inlong"]
}

dag = DAG(
    'dag_cleaner',
    default_args=default_args,
    schedule_interval="*/20 * * * *",
    is_paused_upon_creation=False
)

clean_task = PythonOperator(
    task_id='clean_expired_dags',
    python_callable=clean_expired_dags,
    provide_context=True,
    dag=dag,
)
