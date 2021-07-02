/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { ColumnsType } from 'antd/es/table';
import rulesPattern from '@/utils/pattern';

export const getCreateFormContent = () => {
  const array = [
    {
      type: 'input',
      label: '数据源IP',
      name: 'ip',
      rules: [
        { required: true },
        {
          pattern: rulesPattern.ip,
          message: '请正确输入Ip地址',
        },
      ],
    },
    {
      type: 'inputnumber',
      label: '端⼝',
      name: 'port',
      props: {
        min: 0,
      },
      rules: [
        { required: true },
        {
          pattern: rulesPattern.port,
          message: '请正确输入端口号(1-65535)',
        },
      ],
    },
    {
      type: 'input',
      label: '⽂件路径',
      name: 'filePath',
      rules: [{ required: true }],
      suffix: '填写绝对路径',
    },
  ];

  return array;
};

export const tableColumns: ColumnsType = [
  {
    title: '数据源IP',
    dataIndex: 'ip',
    width: 150,
  },
  {
    title: '端口',
    dataIndex: 'port',
    width: 120,
  },
  {
    title: '⽂件路径',
    dataIndex: 'filePath',
  },
];
