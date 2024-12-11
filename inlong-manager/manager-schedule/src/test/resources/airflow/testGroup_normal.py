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
from datetime import datetime
from croniter import croniter
from airflow.hooks.base_hook import BaseHook
import requests
import pytz

timezone = "Asia/Shanghai"
start_offset_datetime_str = 1731072908243
end_offset_datetime_str = 1731142800000
schedule_interval = "*/1 * * * *"  # Or put cron expression
dag_id = "inlong_offline_task_testGroup_normal"
groupId = "test_offline_1"
connectionId = "inlong_connection"
boundaryType = str("time")

target_timezone = pytz.timezone(timezone)

start_date = datetime.fromtimestamp(start_offset_datetime_str / 1000, tz=target_timezone)
end_date = datetime.fromtimestamp(end_offset_datetime_str / 1000, tz=target_timezone)


def taskFunction(**context):
    print("#########################")
    conn = BaseHook.get_connection(connectionId)
    url = f"http://{conn.host}:{conn.port}/{conn.schema}"
    params = {
        "username": conn.login,
        "password": conn.password
    }
    print("params", params)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/json;charset=UTF-8",
        "tenant": "public",
        "Connection": "close",
        "Priority": "u=0"
    }
    time_interval = get_time_interval(context)
    data = {
        "boundaryType": boundaryType,
        "groupId": groupId,
        "lowerBoundary": time_interval[0],
        "upperBoundary": time_interval[1]
    }
    print("Request Body: ", data)
    response = requests.post(url, params=params, headers=headers, json=data)
    if response.status_code == 200:
        print(response.json())
    else:
        print(response.text)
    print("#########################")


def get_time_interval(context):
    execution_date = context.get('execution_date')
    execution_date = execution_date.astimezone(target_timezone)
    dag = context.get('dag')
    schedule_interval = dag.schedule_interval
    if isinstance(schedule_interval, timedelta):
        return execution_date.timestamp(), (execution_date + schedule_interval).timestamp()
    else:
        cron_expr = dag.schedule_interval
        cron = croniter(cron_expr, execution_date)
        next_run = cron.get_next(datetime)
        return execution_date.timestamp(), next_run.timestamp()


default_args = {
    'owner': 'inlong',
    'start_date': start_date,
    'end_date': end_date,
    'catchup': False,
}

dag = DAG(
    dag_id,
    default_args=default_args,
    schedule_interval=schedule_interval,
    is_paused_upon_creation=False
)

clean_task = PythonOperator(
    task_id=dag_id,
    python_callable=taskFunction,
    provide_context=True,
    dag=dag,
)
