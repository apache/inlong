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


def clean_expired_dags(**context):
    original_time = context.get('execution_date')
    target_timezone = pytz.timezone("Asia/Shanghai")
    utc_time = original_time.astimezone(target_timezone)
    current_time = utc_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    logging.info(f"Current time: {current_time}")
    for dag_file in os.listdir(DAG_PATH):
        if dag_file.endswith(".py") and dag_file.startswith("inlong_offline_task_"):
            with open(DAG_PATH + dag_file, "r") as file:
                line = file.readline()
                while line and "end_offset_datetime_str" not in line:
                    line = file.readline()
                end_date_str = None
                if len(line.split("=")) > 1:
                    end_date_str = line.split("=")[1].strip().strip("\"")
                logging.info(f"DAG end time: {end_date_str}")
                if end_date_str:
                    try:
                        if str(current_time) > str(end_date_str):
                            dag_file_path = os.path.join(DAG_PATH, dag_file)
                            os.remove(dag_file_path)
                            # Optionally, delete the end_date variable
                            logging.info(f"Deleted expired DAG: {dag_file}")
                    except ValueError:
                        logging.error(f"Invalid date format for DAG {dag_file}: {end_date_str}")


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
